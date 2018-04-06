(ns tarantool-clj.connection
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [tarantool-clj.constants :as constants]
            [clojure.string :as string]
            [clojure.data.codec.base64 :as base64]
            [msgpack.core :as msgpack])
  (:import [java.io InputStreamReader]
           [java.net Socket]
           [java.security MessageDigest]))

(defn- get-packet
  [input length]
  (let [bytes (byte-array length)]
    (.read input bytes)
    bytes))

(defn- get-msgpack-packet
  [input]
  (msgpack/unpack input))

(defn- error-or-data
  [header body]
  (let [code (get header (:code constants/USER-KEYS))
        error (get body (:error constants/USER-KEYS))
        ok-code (:ok constants/RESPONSE-CODES)
        data (get body (:data constants/USER-KEYS))]
    (if (not= ok-code code)
      (Exception. (format "Tarantool Protocol Error: 0x%x, %s" code error))
      data)))

(defn- safe-return
  [data]
  (if (= java.lang.Exception (type data))
    (throw data)
    data))

(defn- safe-callback
  [data callback-fn]
  (if (= java.lang.Exception (type data))
    data
    (callback-fn data)))

(defn- get-response
  [input]
  (let [[length header body] (repeatedly 3 #(get-msgpack-packet input))]
    [length header body]))

(defn- send-packet
  [output request-id request-type request-body & [schema-id]]
  (let [body (msgpack/pack request-body)
        request-code (or (get constants/REQUEST-CODES request-type)
                         (throw (Exception. (format "Wrong request type: %s, not one of %s"
                                                    request-type
                                                    (keys constants/REQUEST-CODES)))))

        header (msgpack/pack
                 (cond-> {(:code constants/USER-KEYS) request-code
                          (:sync constants/USER-KEYS) request-id}
                         schema-id (assoc (:schema-id constants/USER-KEYS) schema-id)))

        size (msgpack/pack (+ (count body) (count header)))]
    (doseq [data [size header body]]
      (.write output data))
    (.flush output)))


(defn- xor
  [left right size]
  (take size (map bit-xor left right)))

(defn- authorize!
  [input output salt username password]
  (when (and username password)
    (let [salt-decoded (->> salt
                            .getBytes
                            base64/decode)
          step-1 (.digest (MessageDigest/getInstance "SHA1") (.getBytes password))
          step-2 (.digest (MessageDigest/getInstance "SHA1") step-1)
          step-3 (.digest
                         (doto
                           (MessageDigest/getInstance "SHA1")
                           (.update salt-decoded 0 20)
                           (.update step-2)))

          scramble (byte-array (xor step-1 step-3 constants/SCRAMBLE-SIZE))
          body     {(:username constants/USER-KEYS) username
                    (:tuple constants/USER-KEYS)    ["chap-sha1" scramble]}]
      (send-packet output 0 :auth body)
      (let [[length header body] (get-response input)]
        (safe-return (error-or-data header body))
        (log/debug "Authorized successfully")))))

(defn- greeting!
  [input]
  (log/debug "Starting greeting")
  (let [greeting (vec (get-packet input constants/GREETING-LENGTH))

        version (->> (subvec greeting 0 constants/GREETING-VERSION-LENGTH)
                     (map char)
                     (string/join))
        salt    (->> (subvec greeting
                             constants/GREETING-VERSION-LENGTH
                             (+ constants/GREETING-VERSION-LENGTH
                                constants/GREETING-SALT-LENGTH))
                     (map char)
                     (string/join))]
    (log/debug "Greeting succeeded:" version)
    [version salt]))

(defn- connect!
  [host port username password]
  (log/debug (format "Connecting to %s %s" host port))
  (let [socket (Socket. host port)
        input (io/input-stream socket)
        output (io/output-stream socket)]
    (let [[version salt] (greeting! input)]
      (authorize! input output salt username password)
      [socket input output])))

(defn- raw->response
  [raw]
  (let [[length header body] raw
        response-id (get header (:sync constants/USER-KEYS))
        data (error-or-data header body)]
    [response-id data]))

(defn- get-read-write-loop
  [host port username password auto-reconnect? responses-chan write-chan stop-chan]
  (a/thread
    (loop []
      (let [[socket input output] (try (connect! host port username password)
                                       (catch Exception e
                                         (log/error e "Error while establishing connection")
                                         nil))]
        (if-not socket
          (do
            (log/debug "Can't connect")
            (a/<!! (a/timeout 1000))
            (recur))
          (let [
                write-loop-stop (a/chan)
                write-loop (a/thread
                             (log/debug "Write Loop: started")
                             (loop []
                               (log/debug "Write Loop: waiting for packets")
                               (a/alt!!
                                 write-loop-stop :stopped
                                 write-chan
                                 ([[request-id request-type request-body request-timeout-chan]]
                                   (if (or (nil? request-timeout-chan)
                                           (a/alt!! request-timeout-chan false :default true))
                                     (let [[status res] (try
                                                          [:ok (send-packet output
                                                                            request-id
                                                                            request-type
                                                                            request-body)]
                                                          (catch java.net.SocketException e
                                                            [:stop e])
                                                          (catch Exception e
                                                            [:error e]))]
                                       (case status
                                         :ok (recur)
                                         :error (do (a/>!! responses-chan [request-id res])
                                                    (recur))
                                         :stop :stop))
                                     (recur))))))
                read-loop (a/thread
                            (log/debug "Read Loop: started")
                            (loop []
                              (log/debug "Read Loop: waiting for packets")
                              (let [[status res] (try
                                                   [:ok (get-response input)]
                                                   (catch java.net.SocketException e
                                                     (log/error (format "Exception in read loop %s" e))
                                                     [:stop])
                                                   (catch Exception e
                                                     (log/error (format "Exception in read loop %s" e))
                                                     [:error e]))]
                                (log/debug (format "Read Loop status: %s, result: %s" status res))
                                (case status
                                  :ok (let [data (raw->response res)]
                                        (a/>!! responses-chan data)
                                        (recur))
                                  :stop :stop
                                  :error :error))))]
            (let [status (a/alt!!
                           stop-chan ([_] (do (log/debug "Read Write Loop: stop")
                                              (a/close! write-loop-stop)
                                              (a/<!! write-loop)
                                              (.close input)
                                              (.close output)
                                              (.close socket)
                                              (a/<!! read-loop)
                                              :exit))
                           write-loop ([v] (do (log/debug "Read Write Loop: write")
                                               (a/<!! read-loop)
                                               :recur))
                           read-loop ([v] (do (log/debug "Read Write Loop: read")
                                              (a/close! write-loop-stop)
                                              (a/<!! write-loop)
                                              (when (= :error v)
                                                (.close input)
                                                (.close output)
                                                (.close socket))
                                              :recur)))]
              (a/>!! responses-chan :reset-all)
              (when (and auto-reconnect?
                         (= :recur status))
                (recur)))))))))

(defn- get-request-response-loop
  [request-timeout requests-chan responses-chan write-chan stop-chan]
  (let [timeouts-chan (a/chan 1000)]
    (a/thread
      (loop [requests {} running? true request-id 0]
        (log/debug "Request Response Loop: waiting for event")
        (let [channels (cond-> [requests-chan responses-chan timeouts-chan]
                               running? (conj stop-chan))
              [v chan] (a/alts!! channels)]
          (log/debug "Request Response Loop: new event")
          (condp = chan
            timeouts-chan (do
                            (log/debug "Request Response Loop: timeout event")
                            (let [response-chan (get requests v)]
                              (when response-chan
                                (a/>!! response-chan (Exception. "Timeout")))
                              (when (or running? (seq requests))
                                (recur (dissoc requests v) running? request-id))))
            stop-chan (do
                        (log/debug "Request Response Loop: stop event")
                        (when (seq requests) (recur requests false request-id)))
            requests-chan (do
                            (log/debug "Request Response Loop: request event")
                            (let [request-id* (inc request-id)
                                  [request-type request-body response-chan callback-fn] v
                                  request-timeout-chan (when request-timeout
                                                         (a/timeout request-timeout))]
                              (when request-timeout-chan
                                (a/go (a/<! request-timeout-chan)
                                      (a/>! timeouts-chan request-id*)))
                              (a/>!! write-chan [request-id* request-type request-body request-timeout-chan])
                              (recur (assoc requests request-id* {:chan response-chan :callback-fn callback-fn})
                                     running?
                                     request-id*)))
            responses-chan (do
                             (log/debug "Request Response Loop: response event")
                             (if (= v :reset-all)
                               (do
                                 (doseq [[response-id response-chan] requests]
                                   (a/>!! response-chan (Exception. (format "Internal error"))))
                                 (when running? (recur {} running? request-id)))
                               (let [[response-id data] v
                                     response-chan (:chan (get requests response-id))
                                     callback-fn (:callback-fn (get requests response-id))]
                                 (if response-chan
                                   (a/>!! response-chan (safe-callback data callback-fn))
                                   (log/debug (format "Deadend response %s %s" response-id data)))
                                 (when (or running? (seq requests))
                                   (recur (dissoc requests response-id) running? request-id)))))))))
    (log/debug "Request Response Loop: Exit")))

(defn- event-loop
  [{:keys [host port auto-reconnect? username password request-timeout]}]
  (let [read-write-loop-stop (a/chan)
        write-chan (a/chan 1000)
        read-chan (a/chan 1000)
        responses-chan (a/chan 1000)
        requests-chan (a/chan 1000)
        request-response-loop-stop (a/chan)
        read-write-loop (get-read-write-loop host port username password auto-reconnect? responses-chan write-chan read-write-loop-stop)
        request-response-loop (get-request-response-loop request-timeout requests-chan responses-chan write-chan request-response-loop-stop)
        stop-fn #(do (a/close! request-response-loop-stop)
                     (a/close! read-write-loop-stop))]
    [requests-chan stop-fn]))

(defprotocol TarantoolConnectionProtocol
  (send-request
    [this request-type request-packet]
    [this request-type request-packet callback-fn]))

(def empty-callback-fn
  (fn [v] v))

(defrecord TarantoolConnection [host port username password auto-reconnect? request-timeout async?
                                requests-chan socket-loop-stop-fn]
  component/Lifecycle
  (start [this]
    (log/debug "Starting tarantool connection")
    (let [[requests-chan socket-loop-stop-fn] (event-loop this)]
      (assoc this
        :socket-loop-stop-fn socket-loop-stop-fn
        :requests-chan requests-chan)))
  (stop [this]
    (log/debug "Stopping tarantool connection: ...")
    (socket-loop-stop-fn)
    (log/debug "Stopping tarantool connection: connection stopped")
    (dissoc this :requests-chan :socket-loop-stop-fn))
  TarantoolConnectionProtocol
  (send-request [this request-type request-body]
    (let [response-chan (a/chan)]
      (a/>!! requests-chan [request-type request-body response-chan empty-callback-fn])
      (if async?
        response-chan
        (safe-return (a/<!! response-chan)))))
  (send-request [this request-type request-body callback-fn]
    (let [response-chan (a/chan)]
      (a/>!! requests-chan [request-type request-body response-chan callback-fn])
      (if async?
        response-chan
        (safe-return (a/<!! response-chan))))))

(defn new-tarantool-connection
  [config]
  (let [config* (merge constants/DEFAULT-CONNECTION config)]
    (-> config*
        map->TarantoolConnection
        component/start)))