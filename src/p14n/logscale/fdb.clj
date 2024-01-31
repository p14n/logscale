(ns p14n.logscale.fdb
  (:import com.apple.foundationdb.FDB
           io.github.aleris.testcontainers.containers.FoundationDBContainer
           com.apple.foundationdb.tuple.Tuple
           org.testcontainers.utility.DockerImageName))

(defonce fdb (FDB/selectAPIVersion 710))

(defn start-container []
  (let [container (doto (FoundationDBContainer. (DockerImageName/parse "foundationdb/foundationdb:7.1.23"))
                    (.start))]
    (println "Started container" (.getContainerIpAddress container) (.getMappedPort container))
    container))

(defn db-from-container [container]
  (let [cluster-file (.getClusterFilePath container)
        db (.open fdb cluster-file)]
    (println "Opened db" db)
    db))

(defn db-from-local []
  (let [db (.open fdb)]
    (println "Opened db" db)
    db))

(defn transact!
  [db f]
  (.run db
        (reify
          java.util.function.Function
          (apply [_ tr]
            (println "Running transaction" tr)
            (f tr)))))

(defn pack-tuple [k]
  (.pack (Tuple/from (into-array [k]))))

(defn -set! [tr key-values]
  (run! (fn [[k v]]
          (println "Setting" k v)
          (let [key (pack-tuple k)
                value (pack-tuple v)]
            (println "Setting" key value)
            (.set tr key value)))
        key-values))

(defn -get [tr key]
  (->> key
       (pack-tuple)
       (.get tr)
       (.join)))

(defn bytes-to-string [bytes]
  (-> bytes
      (Tuple/fromBytes)
      (.getString 0)))
