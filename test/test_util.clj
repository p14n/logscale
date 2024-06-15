(ns test-util
  (:require [datascript.core :as d]
            [clojure.test :refer :all]))

(def schema {:aka {:db/cardinality :db.cardinality/many}})
(def opts {:branching-factor 512})

(defn add-person [conn name age aka]
  (d/transact! conn [{:db/id -1
                      :name  name
                      :age   age
                      :aka   [aka]}]))
(defn get-person [db name]
  (d/q `[:find  ?n ?a
         :where [?e :name ~name]
         [?e :name ?n]
         [?e :age  ?a]]
       db))