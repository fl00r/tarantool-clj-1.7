(ns tarantool-clj.client-async-test
  (:require [tarantool-clj
             [client :as client]
             [tuple-space :as tuple-space]
             [space :as space]
             [test-utils :refer [with-truncated-tarantool-async]]]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as a])
  (:refer-clojure :exclude [eval update replace]))

(def config (clojure.edn/read-string (slurp "config/test_async.clj")))

(defn client []
  (-> config
      :tarantool
      (client/new-client)
      (component/start)))

(use-fixtures :each (partial with-truncated-tarantool-async client))

(deftest tuple-space
  (let [space (->> config
                   :tester-tuple-space
                   (tuple-space/new-tuple-space (client))
                   (component/start))]
    (are [x y] (= x y)
               (a/<!! (tuple-space/insert space [1 "Steve" "Buscemi"]))
               [[1 "Steve" "Buscemi"]]

               (a/<!! (tuple-space/insert space [2 "Steve" "Jobs"]))
               [[2 "Steve" "Jobs"]]

               (a/<!! (tuple-space/insert space [3 "Tim" "Roth"]))
               [[3 "Tim" "Roth"]]

               (a/<!! (tuple-space/select space 0 [1]))
               [[1 "Steve" "Buscemi"]])

    (is (= (type (a/<!!(tuple-space/insert space [3 "Tim" "Roth"]))) java.lang.Exception))

    (are [x y] (= x y)
               (a/<!! (tuple-space/select space 1 ["Steve"] {:iterator :eq}))
               [[1 "Steve" "Buscemi"] [2 "Steve" "Jobs"]]

               (a/<!! (tuple-space/delete space [1]))
               [[1 "Steve" "Buscemi"]]

               (a/<!! (tuple-space/select space 1 ["Steve"] {:iterator :eq}))
               [[2 "Steve" "Jobs"]]

               (a/<!! (tuple-space/update space [2] [["=" 2 "Ballmer"]]))
               [[2 "Steve" "Ballmer"]]

               (a/<!! (tuple-space/select space 1 ["Steve"]))
               [[2 "Steve" "Ballmer"]])))



(deftest space
  (let [space (->> config
                   :tester-space
                   (space/new-space (client))
                   (component/start))]
    (are [x y] (= x y)
               (a/<!! (space/insert space
                             {:id 1
                              :first-name "Steve"
                              :second-name "Buscemi"}))
               '({:id 1 :first-name "Steve" :second-name "Buscemi"})

               (a/<!! (space/insert space
                             {:id 2
                              :first-name "Steve"
                              :second-name "Jobs"}))
               '({:id 2 :first-name "Steve" :second-name "Jobs"})

               (a/<!! (space/insert space
                             {:id 3
                              :first-name "Tim"
                              :second-name "Roth"}))
               '({:id 3 :first-name "Tim" :second-name "Roth"}))

    (is (= (type (a/<!!(space/insert space {:id 3
                                            :first-name "Tim"
                                            :second-name "Roth"}))) java.lang.Exception))
    (are [x y] (= x y)
               (a/<!! (space/insert space
                             {:id 4
                              :first-name "Bill"
                              :second-name "Gates"
                              :_tail [1 2 3 4 5]}))
               '({:id 4 :first-name "Bill" :second-name "Gates" :_tail (1 2 3 4 5)})

               (a/<!! (space/select-first space
                                   {:id 1}))
               {:id 1 :first-name "Steve" :second-name "Buscemi"}

               (a/<!! (space/select space
                             {:first-name "Steve"}))
               '({:id 1 :first-name "Steve" :second-name "Buscemi"}
                  {:id 2 :first-name "Steve" :second-name "Jobs"})

               (a/<!! (space/select space
                             {:first-name "Steve" :second-name "Jobs"}))
               '({:id 2 :first-name "Steve" :second-name "Jobs"})

               (a/<!! (space/select space
                             {:first-name "Steve"}
                             {:iterator :eq}))
               '({:id 1 :first-name "Steve" :second-name "Buscemi"}
                  {:id 2 :first-name "Steve" :second-name "Jobs"})

               (a/<!! (space/select space
                             {:first-name "Steve"}
                             {:iterator :eq :offset 1 :limit 100}))
               '({:id 2 :first-name "Steve" :second-name "Jobs"})

               (a/<!! (space/update space
                             {:id 2}
                             {:second-name ["=" "Ballmer"]}))
               '({:id 2 :first-name "Steve" :second-name "Ballmer"})

               (a/<!! (space/update space
                             {:id 2}
                             {:second-name [":" 3 4 "dwin"]
                              :first-name [":" 3 4 "phen"]}))
               '({:id 2 :first-name "Stephen" :second-name "Baldwin"})

               (a/<!! (space/delete space
                             {:id 3}))
               '({:id 3 :first-name "Tim" :second-name "Roth"})

               (a/<!! (space/replace space
                              {:id 4 :first-name "Bill" :second-name "Murray"}))
               '({:id 4 :first-name "Bill" :second-name "Murray"})

               (a/<!! (space/eval space
                           "function ping()
                                 return 'pong'
                               end"))
               []

               (a/<!! (space/call space
                           "ping"))
               ["pong"])))