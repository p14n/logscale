(ns atom-test
  (:require [datascript.core :as d]
            [atom-storage :as as]
            [clojure.test :refer :all]
            [test-util :as tu]))

(deftest atom-test
  (let [atom-map (atom {})
        s (as/make-storage atom-map)]
    (testing "Database saves to an atom"
      (let [conn (d/create-conn tu/schema tu/opts)]
        (tu/add-person conn "Dean" 94 "Me")
        (d/store (d/db conn) s)
        (is (= #{["Dean" 94]} (tu/get-person (d/db conn) "Dean")))
        (println "x")
        (is (seq @atom-map))))
    (testing "Database restores from an atom"
      (let [conn (d/restore-conn s (assoc tu/opts :schema tu/schema))]
        (is (= #{["Dean" 94]} (tu/get-person (d/db conn) "Dean")))))))

(deftest atom-test-many
  (let [atom-map (atom {})
        s (as/make-storage atom-map)]
    (testing "Database saves to an atom"
      (let [conn (d/create-conn tu/schema tu/opts)]
        (tu/add-person conn "Dean" 70 "Me")
        (d/store (d/db conn) s)
        (tu/add-person conn "Dean" 71 "Me")
        (d/store (d/db conn) s)
        (tu/add-person conn "Dean" 72 "Me")
        (d/store (d/db conn) s)
        (tu/add-person conn "Dean" 73 "Me")
        (d/store (d/db conn) s)
        (tu/add-person conn "Dean" 74 "Me")
        (d/store (d/db conn) s)
        (tu/add-person conn "Dean" 75 "Me")
        (d/store (d/db conn) s)
        (tu/add-person conn "Dean" 76 "Me")
        (d/store (d/db conn) s)))
    (testing "Database restores from an atom"
      (let [conn (d/restore-conn s (assoc tu/opts :schema tu/schema))]
        (is (= #{70 71 72 73 74 75 76} (->> (tu/get-person (d/db conn) "Dean")
                                            (map second)
                                            (into #{}))))))))
