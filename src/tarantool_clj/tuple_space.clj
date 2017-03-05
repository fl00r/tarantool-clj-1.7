(ns tarantool-clj.tuple-space
  (:require [tarantool-clj
             [constants :as constants]
             [client :as client]]
            [com.stuartsierra.component :as component])
  (:refer-clojure :exclude [eval update replace]))

(defprotocol TupleSpaceProtocol
  (select
    [this index-id key-tuple]
    [this index-id key-tuple opts])
  (insert [this data-tuple])
  (replace [this data-tuple])
  (update
    [this key-tuple ops-tuples]
    [this index-id key-tuple ops-tuples])
  (delete
    [this key-tuple]
    [this index-id key-tuple])
  (upsert [this data-tuple ops-tuples])
  (call
    [this function-name]
    [this function-name args])
  (eval
    [this expression]
    [this expression args]))

(defn select*
  [{:keys [client id] :as tuple-space}
   index-id
   tuple
   {:keys [limit offset iterator] :or {limit Integer/MAX_VALUE offset 0 iterator :eq}}]
  (let [iterator-id (get constants/ITERATORS iterator)]
    (when-not iterator-id
      (throw (Exception.
              (str
               "Wrong iterator "
               iterator
               ", valid iterators are: "
               (keys constants/ITERATORS)))))
    (client/select client id index-id limit offset iterator-id tuple)))

(defrecord TupleSpace [id client]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  TupleSpaceProtocol
  (select [this index-id key-tuple]
    (select this index-id key-tuple {}))
  (select [this index-id key-tuple opts]
    (select* this index-id key-tuple opts))
  (insert [{:keys [client]} data-tuple]
    (client/insert client id data-tuple))
  (replace [{:keys [client]} data-tuple]
    (client/replace client id data-tuple))
  (update [this key-tuple ops-tuples]
    (update this 0 key-tuple ops-tuples))
  (update [{:keys [client]} index-id key-tuple ops-tuples]
    (client/update client id index-id key-tuple ops-tuples))
  (delete [this key-tuple]
    (delete this 0 key-tuple))
  (delete [{:keys [client]} index-id key-tuple]
    (client/delete client id index-id key-tuple))
  (upsert [{:keys [client]} data-tuple ops-tuples])
  (call [this function-name]
    (call this function-name []))
  (call [{:keys [client]} function-name args]
    (client/call client function-name args))
  (eval [this expression]
    (eval this expression []))
  (eval [{:keys [client]} expression args]
    (client/eval client expression args)))


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
