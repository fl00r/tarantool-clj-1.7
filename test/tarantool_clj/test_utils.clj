(ns tarantool-clj.test-utils
  (:require [com.stuartsierra.component :as component]
            [tarantool-clj.client :as client]))

(defn with-truncated-tarantool
  [client f]
  (client/eval client (slurp (clojure.java.io/resource "scripts/app.lua")) [])
  (client/call client "create_testing_space" [])
  (try
    (f)
    (finally
      (client/call client "drop_testing_space" []))))
