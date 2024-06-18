(ns fdb-test
  (:require [datascript.core :as d]
            [clojure.test :refer :all]
            [test-util :as tu]
            [taoensso.nippy :as nippy]
            [p14n.logscale.core :as sut]
            [p14n.logscale.fdb :as fdb])
  (:import com.apple.foundationdb.tuple.Tuple))

(defn bytes->tuple->nippy-thaw [bytes]
  (some-> bytes
          (Tuple/fromBytes)
          (.getBytes 0)
          (nippy/thaw)))

(defn reset [keys]
  (with-open [db (fdb/open-db)]
    (fdb/transact! db #(do
                         (println "Clearing" % keys)
                         (fdb/-clear % keys)))))

(deftest single-insert-test
  (let [fdb (sut/create-datascript-function {:schema tu/schema
                                             :root-keys ["test" "string"]
                                             :datascript-opts tu/opts})]
    (testing "Database saves to fdb"
      (reset ["test" "string" 0])
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
  (let [fdb (sut/create-datascript-function {:schema tu/schema
                                             :root-keys ["test" "nippy"]
                                             :datascript-opts tu/opts
                                             :freeze-fn nippy/freeze
                                             :thaw-fn bytes->tuple->nippy-thaw})]
    (testing "Database saves nippy to fdb"
      (reset ["test" "nippy" 0])
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


(deftest dual-datascript-fdb-insert-test
  (let [opts {:schema tu/schema
              :root-keys ["test" "dual"]
              :datascript-opts tu/opts
              :freeze-fn nippy/freeze
              :thaw-fn bytes->tuple->nippy-thaw}]
    (testing "Database saves nippy and extra key to fdb"
      (reset ["test" "dual" 0])
      (reset ["test" "dual" "hello"])

      (sut/with-transaction
        (fn [transaction]
          (sut/with-connection
            (fn [conn]
              (d/transact! conn [{:db/id -1
                                  :name  "Dean"
                                  :age   49
                                  :aka   ["Me"]}]))
            opts
            transaction)
          (fdb/-set! transaction [[["test" "dual" "hello"] (nippy/freeze {:name "Tom" :age 4 :aka ["Not Me"]})]])))

      (sut/with-transaction
        (fn [transaction]
          (is (= #{["Dean" 49 "Me"]}
                 (sut/with-connection
                   (fn [conn]
                     (d/q `[:find  ?n ?a ?s
                            :where [?e :name "Dean"]
                            [?e :name ?n]
                            [?e :age  ?a]
                            [?e :aka  ?s]]
                          (d/db conn)))
                   opts
                   transaction)))
          (is (= {:name "Tom" :age 4 :aka ["Not Me"]}
                 (-> (fdb/-get transaction ["test" "dual" "hello"])
                     (bytes->tuple->nippy-thaw)))))))))


(deftest pull-query-test
  (let [fdb (sut/create-datascript-function {:schema {:owners {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
                                                      :registered-address {:db/valueType :db.type/ref}}
                                             :root-keys ["test" "pull"]
                                             :datascript-opts tu/opts
                                             :freeze-fn nippy/freeze
                                             :thaw-fn bytes->tuple->nippy-thaw})]
    (testing "Database pull queries example"
      (reset ["test" "pull" 0])
      (fdb
       #(d/transact! % [{:db/id -1 :name  "Dean"}
                        {:db/id -2 :account-type :isa :owners [-1] :registered-address -3}
                        {:db/id -3 :postcode "IPXX 21"}]))

      (is (= [{:owners [{:name "Dean"}] :registered-address {:postcode "IPXX 21"}}]
             (fdb #(d/q '[:find [(pull ?e [{:owners [:name] :registered-address [:postcode]}]) ...]
                          :where [?e :account-type :isa]] (d/db %)))))

      (is (= [{:name "Dean" :owns [{:account-type :isa :registered-address {:postcode "IPXX 21"}}]}]
             (fdb #(d/q '[:find [(pull ?e [:name {[:_owners :as :owns]
                                                  [:account-type {:registered-address [:postcode]}]}]) ...]
                          :where [?e :name "Dean"]] (d/db %))))))))
