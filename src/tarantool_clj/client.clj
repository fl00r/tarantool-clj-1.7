(ns tarantool-clj.client
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [tarantool-clj.connection :as connection]
            [tarantool-clj.constants :as constants])
  (:refer-clojure :exclude [eval update replace]))

(defn- assoc-user-key
  [packet user-key value]
  (let [code (or (get constants/USER-KEYS user-key)
                 (throw (Exception. (format "Wrong user-key %s, not one of %s"
                                            user-key
                                            (keys constants/USER-KEYS)))))]
    (assoc packet code value)))

(defn- assoc-space-id
  [packet space-id]
  (assoc-user-key packet :space-id space-id))

(defn- assoc-index-id
  [packet index-id]
  (assoc-user-key packet :index-id index-id))

(defn- assoc-limit
  [packet limit]
  (assoc-user-key packet :limit limit))

(defn- assoc-offset
  [packet offset]
  (assoc-user-key packet :offset offset))

(defn- assoc-iterator
  [packet iterator]
  (assoc-user-key packet :iterator iterator))

(defn- assoc-key
  [packet key-tuple]
  (assoc-user-key packet :key key-tuple))

(defn- assoc-tuple
  [packet data-tuple]
  (assoc-user-key packet :tuple data-tuple))

(defn- assoc-ops
  [packet ops-tuples]
  (assoc-user-key packet :ops ops-tuples))

(defn- assoc-function-name
  [packet function-name]
  (assoc-user-key packet :function-name function-name))

(defn- assoc-expression
  [packet expression]
  (assoc-user-key packet :expression expression))

;;
;; https://tarantool.org/en/doc/dev_guide/internals_index.html#box-protocol-iproto-protoco
;;
(defprotocol TarantoolClientProtocol
  (select
    [this space-id index-id limit offset iterator key-tuple]
    [this space-id index-id limit offset iterator key-tuple callback-fn])
  (insert
    [this space-id data-tuple]
    [this space-id data-tuple callback-fn])
  (replace
    [this space-id data-tuple])
  (replace*
    [this space-id data-tuple callback-fn])
  (update
    [this space-id index-id key-tuple ops-tuples]
    [this space-id index-id key-tuple ops-tuples callback-fn])
  (delete
    [this space-id index-id key-tuple]
    [this space-id index-id key-tuple callback-fn])
  (upsert
    [this space-id data-tuple ops-tuples]
    [this space-id data-tuple ops-tuples callback-fn])
  (call
    [this function-name args-tuple]
    [this function-name args-tuple callback-fn])
  (eval
    [this expression args-tuple]
    [this expression args-tuple callback-fn]))

(defrecord TarantoolClient [config connection]
  component/Lifecycle
  (start [this]
    (let [connection (connection/new-tarantool-connection config)]
      (assoc this :connection connection)))
  (stop [this]
    (component/stop connection)
    (dissoc this connection))
  TarantoolClientProtocol
  (select [this space-id index-id limit offset iterator key-tuple]
    (let [request-type :select
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-index-id index-id)
                           (assoc-limit limit)
                           (assoc-offset offset)
                           (assoc-iterator iterator)
                           (assoc-key key-tuple))]
      (connection/send-request connection request-type request-body)))
  (select [this space-id index-id limit offset iterator key-tuple callback-fn]
    (let [request-type :select
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-index-id index-id)
                           (assoc-limit limit)
                           (assoc-offset offset)
                           (assoc-iterator iterator)
                           (assoc-key key-tuple))]
      (connection/send-request connection request-type request-body callback-fn)))
  (insert [this space-id data-tuple]
    (let [request-type :insert
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-tuple data-tuple))]
      (connection/send-request connection request-type request-body)))
  (insert [this space-id data-tuple callback-fn]
    (let [request-type :insert
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-tuple data-tuple))]
      (connection/send-request connection request-type request-body callback-fn)))
  (replace [this space-id data-tuple]
    (let [request-type :replace
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-tuple data-tuple))]
      (connection/send-request connection request-type request-body)))
  (replace* [this space-id data-tuple callback-fn]
    (let [request-type :replace
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-tuple data-tuple))]
      (connection/send-request connection request-type request-body callback-fn)))
  (update [this space-id index-id key-tuple ops-tuples]
    (let [request-type :update
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-index-id index-id)
                           (assoc-key key-tuple)
                           (assoc-tuple ops-tuples))]
      (connection/send-request connection request-type request-body)))
  (update [this space-id index-id key-tuple ops-tuples callback-fn]
    (let [request-type :update
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-index-id index-id)
                           (assoc-key key-tuple)
                           (assoc-tuple ops-tuples))]
      (connection/send-request connection request-type request-body callback-fn)))
  (delete [this space-id index-id key-tuple]
    (let [request-type :delete
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-index-id index-id)
                           (assoc-key key-tuple))]
      (connection/send-request connection request-type request-body)))
  (delete [this space-id index-id key-tuple callback-fn]
    (let [request-type :delete
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-index-id index-id)
                           (assoc-key key-tuple))]
      (connection/send-request connection request-type request-body callback-fn)))
  (upsert [this space-id data-tuple ops-tuples]
    (let [request-type :upsert
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-tuple data-tuple)
                           (assoc-ops ops-tuples))]
      (connection/send-request connection request-type request-body)))
  (upsert [this space-id data-tuple ops-tuples callback-fn]
    (let [request-type :upsert
          request-body (-> {}
                           (assoc-space-id space-id)
                           (assoc-tuple data-tuple)
                           (assoc-ops ops-tuples))]
      (connection/send-request connection request-type request-body callback-fn)))
  (call [this function-name args-tuple]
    (let [request-type :call
          request-body (-> {}
                           (assoc-function-name function-name)
                           (assoc-tuple args-tuple))]
      (connection/send-request connection request-type request-body)))
  (call [this function-name args-tuple callback-fn]
    (let [request-type :call
          request-body (-> {}
                           (assoc-function-name function-name)
                           (assoc-tuple args-tuple))]
      (connection/send-request connection request-type request-body callback-fn)))
  (eval [this expression args-tuple]
    (let [request-type :eval
          request-body (-> {}
                           (assoc-expression expression)
                           (assoc-tuple args-tuple))]
      (connection/send-request connection request-type request-body)))
  (eval [this expression args-tuple callback-fn]
    (let [request-type :eval
          request-body (-> {}
                           (assoc-expression expression)
                           (assoc-tuple args-tuple))]
      (connection/send-request connection request-type request-body callback-fn))))

(defn new-client
  [config]
  (component/start (map->TarantoolClient {:config config})))

