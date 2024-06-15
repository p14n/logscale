(ns fdb-test
  (:require [datascript.core :as d]
            [clojure.test :refer :all]
            [p14n.logscale.core :as c]
            [test-util :as tu]
            [taoensso.nippy :as nippy]
            [p14n.logscale.fdb :as fdb])
  (:import com.apple.foundationdb.tuple.Tuple))

(defn reset [keys]
  (with-open [db (fdb/open-db)]
    (fdb/transact! db #(do
                         (println "Clearing" % keys)
                         (fdb/-clear % keys)))))

(deftest single-insert-test
  (let [fdb (c/create-db-function {:schema tu/schema
                                   :root-keys [0 1]
                                   :datascript-opts tu/opts})]
    (testing "Database saves to fdb"
      (reset [0 1 0])
      (fdb
       #(d/transact! % [{:db/id -1
                         :name  "Dean"
                         :age   94
                         :aka   ["Me"]}]))
      (is (= #{["Dean" 94 "Me"]}
             (fdb #(d/q `[:find  ?n ?a ?s
                          :where [?e :name "Dean"]
                          [?e :name ?n]
                          [?e :age  ?a]
                          [?e :aka  ?s]]
                        (d/db %))))))))

(deftest single-insert-test-with-nippy
  (let [fdb (c/create-db-function {:schema tu/schema
                                   :root-keys [0 2]
                                   :datascript-opts tu/opts
                                   :freeze-fn nippy/freeze
                                   :thaw-fn #(some-> %
                                                     (Tuple/fromBytes)
                                                     (.getBytes 0)
                                                     (nippy/thaw))})]
    (testing "Database saves nippy to fdb"
      (reset [0 2 0])
      (fdb
       #(d/transact! % [{:db/id -1
                         :name  "Dean"
                         :age   49
                         :aka   ["Me"]}]))
      (is (= #{["Dean" 49 "Me"]}
             (fdb #(d/q `[:find  ?n ?a ?s
                          :where [?e :name "Dean"]
                          [?e :name ?n]
                          [?e :age  ?a]
                          [?e :aka  ?s]]
                        (d/db %))))))))
