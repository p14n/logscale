# Logscale

[Datascript](https://github.com/tonsky/datascript) with a [foundationdb](https://www.foundationdb.org/) backend.

See [tests](test/fdb_test.clj) for examples 

```clojure
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
```
