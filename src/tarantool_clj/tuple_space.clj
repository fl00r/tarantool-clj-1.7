(ns tarantool-clj.tuple-space
  (:require [tarantool-clj
             [constants :as constants]
             [client :as client]]
            [com.stuartsierra.component :as component])
  (:refer-clojure :exclude [eval update replace]))

(defprotocol TupleSpaceProtocol
  (select
    [this index-id key-tuple]
    [this index-id key-tuple opts]
    [this index-id key-tuple opts callback-fn])
  (insert
    [this data-tuple]
    [this data-tuple callback-fn])
  (replace
    [this data-tuple]
    [this data-tuple callback-fn])
  (update
    [this key-tuple ops-tuples]
    [this index-id key-tuple ops-tuples]
    [this index-id key-tuple ops-tuples callback-fn])
  (delete
    [this key-tuple]
    [this index-id key-tuple]
    [this index-id key-tuple callback-fn])
  (upsert
    [this data-tuple ops-tuples])
  (call
    [this function-name]
    [this function-name args]
    [this function-name args callback-fn])
  (eval
    [this expression]
    [this expression args]
    [this expression args callback-fn]))

(defn- select*
  [{:keys [client id] :as tuple-space}
   index-id
   tuple
   {:keys [limit offset iterator] :or {limit Integer/MAX_VALUE offset 0 iterator :eq}}
   callback-fn]
  (let [iterator-id (get constants/ITERATORS iterator)]
    (when-not iterator-id
      (throw (Exception.
              (format "Wrong iterator %s, not one of %s"
                      iterator (keys constants/ITERATORS)))))
    (client/select client id index-id limit offset iterator-id tuple callback-fn)))

(defrecord TupleSpace [id client]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  TupleSpaceProtocol
  (select [this index-id key-tuple]
    (select this index-id key-tuple {}))
  (select [this index-id key-tuple opts]
    (select* this index-id key-tuple opts (fn [v] v)))
  (select [this index-id key-tuple opts callback-fn]
    (select* this index-id key-tuple opts callback-fn))
  (insert [{:keys [client]} data-tuple]
    (client/insert client id data-tuple))
  (insert [{:keys [client]} data-tuple callback-fn]
    (client/insert client id data-tuple callback-fn))
  (replace [{:keys [client]} data-tuple]
    (client/replace client id data-tuple))
  (replace [{:keys [client]} data-tuple callback-fn]
    (client/replace* client id data-tuple callback-fn))
  (update [this key-tuple ops-tuples]
    (update this 0 key-tuple ops-tuples))
  (update [{:keys [client]} index-id key-tuple ops-tuples]
    (client/update client id index-id key-tuple ops-tuples))
  (update [{:keys [client]} index-id key-tuple ops-tuples callback-fn]
    (client/update client id index-id key-tuple ops-tuples callback-fn))
  (delete [this key-tuple]
    (delete this 0 key-tuple))
  (delete [{:keys [client]} index-id key-tuple]
    (client/delete client id index-id key-tuple))
  (delete [{:keys [client]} index-id key-tuple callback-fn]
    (client/delete client id index-id key-tuple callback-fn))
  (upsert [{:keys [client]} data-tuple ops-tuples])
  (call [this function-name]
    (call this function-name []))
  (call [{:keys [client]} function-name args]
    (client/call client function-name args))
  (call [{:keys [client]} function-name args callback-fn]
    (client/call client function-name args callback-fn))
  (eval [this expression]
    (eval this expression []))
  (eval [{:keys [client]} expression args]
    (client/eval client expression args))
  (eval [{:keys [client]} expression args callback-fn]
    (client/eval client expression args callback-fn)))


(defn new-spaces-tuple-space
  [client]
  (->(map->TupleSpace {:id constants/SPACES-SPACE-ID
                       :client client})
     component/start))

(defn new-indexes-tuple-space
  [client]
  (-> (map->TupleSpace {:id constants/INDEXES-SPACE-ID
                        :client client})
      component/start))

(defn new-tuple-space
  [client id]
  (-> (map->TupleSpace {:client client
                        :id id})
      component/start))
