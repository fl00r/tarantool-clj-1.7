(ns tarantool-clj.test-utils
  (:require [com.stuartsierra.component :as component]
            [tarantool-clj.client :as client]
            [clojure.core.async :as a]))

(defn with-truncated-tarantool
  [client f]
  (let [client (client)]
    (client/eval client (slurp (clojure.java.io/resource "scripts/app.lua")) [])
    (client/call client "create_testing_space" [])
    (try
      (f)
      (finally
        (client/call client "drop_testing_space" [])))))

(defn with-truncated-tarantool-async
  "Для асихрона нужна отдельная функция: чтобы дождаться создания спейса перед началом теста"
  [client f]
  (let [client (client)]
    (a/<!! (client/eval client (slurp (clojure.java.io/resource "scripts/app.lua")) []))
    (a/<!! (client/call client "create_testing_space" []))
    (try
      (f)
      (finally
        (a/<!! (client/call client "drop_testing_space" []))))))
