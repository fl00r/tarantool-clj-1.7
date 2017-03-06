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
      (Exception. (format "Tarantool Prtotocol Error: 0x%x, %s" code error))
      data)))

(defn- safe-return
  [data]
  (if (= java.lang.Exception (type data))
    (throw data)
    data))

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
        header (cond-> {(:code constants/USER-KEYS) request-code
                        (:sync constants/USER-KEYS) request-id}
                 schema-id (assoc (:schema-id constants/USER-KEYS) schema-id)
                 true (msgpack/pack))
        size (-> (+ (count body) (count header))
                 (msgpack/pack))]
    (doseq [data [size header body]]
      (.write output data))
    (.flush output)))


(defn- xor
  [left right size]
  (->> (map bit-xor left right)
       (take size)))

(defn- authorize!
  [input output salt username password]
  (when (and username password)
    (let [salt-decoded (->> salt
                            .getBytes
                            base64/decode)
          step-1 (.digest (MessageDigest/getInstance "SHA1") (.getBytes password))
          step-2 (.digest (MessageDigest/getInstance "SHA1") step-1)
          step-3 (-> (doto (MessageDigest/getInstance "SHA1")
                       (.update salt-decoded 0 20)
                       (.update step-2))
                     (.digest))
          scramble (->> (xor step-1 step-3 constants/SCRAMBLE-SIZE) (byte-array))
          body {(:username constants/USER-KEYS) username
                  (:tuple constants/USER-KEYS) ["chap-sha1" scramble]}]
      (send-packet output 0 :auth body)
      (let [[length header body] (get-response input)]
        (-> (error-or-data header body) (safe-return))
        (log/debug "Authorized successfully")))))

(defn- greeting!
  [input]
  (let [greeting (-> (get-packet input constants/GREETING-LENGTH)
                     (vec))
        version (->> (subvec greeting 0 constants/GREETING-VERSION-LENGTH)
                     (map char)
                     (string/join))
        salt (->> (subvec greeting
                          constants/GREETING-VERSION-LENGTH
                          (+ constants/GREETING-VERSION-LENGTH
                             constants/GREETING-SALT-LENGTH))
                  (map char)
                  (string/join))]
    (log/debug "Greeting succeeded:" version)
    [version salt]))

(defn- connect!
  [host port username password]
  (let [socket (Socket. host port)
        input (io/input-stream socket)
        output (io/output-stream socket)]
    (let [[version salt] (greeting! input)]
      (authorize! input output salt username password)
      [socket input output])))

(defn- start-socket-loop
  [{:keys [host port auto-reconnect? username password request-timeout]}]
  (let [requests-chan (a/chan 1000)
        responses-chan (a/chan 1000)
        request-id 0
        stop-chan (a/chan)
        [socket input output] (connect! host port username password)]
    (let [requests-thread
          (a/go-loop [requests {} stopping? false request-id 0]
            (if stopping?
              (when (seq requests)
                (log/debug (format "Stopping tarantool connection: waiting for %s responses %s" (count requests) requests))
                (let [[length header body] (a/<! responses-chan)
                      response-id (get header (:sync constants/USER-KEYS))
                      chan (get requests response-id)
                      data (error-or-data header body)]
                  (log/debug (format "Response %s is received with body %s and header %s" response-id body header))
                  (a/>! chan data)
                  (recur (dissoc requests response-id) stopping? request-id)))
              (a/alt! requests-chan
                      ([v]
                       (if (nil? v)
                         (do
                           (log/debug "Stopping tarantool connection: request thread going to stopping mode")
                           (recur requests true request-id))
                         (let [request-id* (inc request-id)
                               [request-type request-body chan] v]
                           (log/debug (format "Request %s is sent" request-id))
                           (send-packet output
                                        request-id
                                        request-type
                                        request-body)
                           (recur (assoc requests request-id chan)
                                  stopping?
                                  request-id*))))
                      responses-chan
                      ([v]
                       (let [[length header body] v
                             response-id (get header (:sync constants/USER-KEYS))
                             chan (get requests response-id)
                             data (error-or-data header body)]
                         (log/debug (format "Response %s is received with body %s and data %s" response-id body data))
                         (a/>! chan data)
                         (recur (dissoc requests response-id)
                                stopping?
                                request-id))))))

          responses-thread
          (a/thread
            (loop []
              (let [data (try
                           (get-response input)
                           (catch java.net.SocketException e
                             (a/alt!!
                               stop-chan ([_] (log/debug "Socket closed explicitly"))
                               (a/timeout 100) ([_] (log/error "Socket closed unexpectedly")))
                             :error)
                           (catch Exception e
                             e))]
                (if (= :error data)
                  :error ;; fix it!
                  (do
                    (a/>!! responses-chan data)
                    (recur))))))

          stop-fn (fn []
                    (log/debug "Stopping tarantool connection: closing requests channel")
                    (a/close! requests-chan)
                    (log/debug "Stopping tarantool connection: waiting for requests thread")
                    (a/<!! requests-thread)
                    (log/debug "Stopping tarantool connection: stopping responses channel")
                    (a/close! responses-chan)
                    (log/debug "Stopping tarantool connection: closing stop chan")
                    (a/close! stop-chan)
                    (log/debug "Stopping tarantool connection: closing output stream")
                    (.close output)
                    (log/debug "Stopping tarantool connection: closing input stream")
                    (.close input)
                    (log/debug "Stopping tarantool connection: closing socket")
                    (.close socket)
                    (log/debug "Stopping tarantool connection: wainting for responses thread")
                    (a/<!! responses-thread))]
      [requests-chan stop-fn])))

(defprotocol TarantoolConnectionProtocol
  (send-request [this request-type request-packet]))

(defrecord TarantoolConnection [host port auto-reconnect? request-timeout async?
                                requests-chan socket-loop-stop-fn]
  component/Lifecycle
  (start [this]
    (log/debug "Starting tarantool connection")
    (let [[requests-chan socket-loop-stop-fn] (start-socket-loop this)]
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
      (a/>!! requests-chan [request-type request-body response-chan])
      (if async?
        response-chan
        (safe-return (a/<!! response-chan))))))

(defn new-tarantool-connection
  [config]
  (let [config* (-> (merge constants/DEFAULT-CONNECTION config))]
    (-> config*
        map->TarantoolConnection
        component/start)))
