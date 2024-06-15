(ns p14n.logscale.fdb
  (:import com.apple.foundationdb.FDB
           com.apple.foundationdb.tuple.Tuple))

(defonce fdb (FDB/selectAPIVersion 710))

(defn open-db
  ([cluster-file] (.open fdb cluster-file))
  ([] (.open fdb)))

(defn transact!
  [db f]
  (.run db
        (reify
          java.util.function.Function
          (apply [_ tr]
            (f tr)))))

(defn pack-tuple [k]
  (.pack (Tuple/from (into-array [k]))))

(defn -set! [tr key-values]
  (run! (fn [[k v]]
          (let [key (pack-tuple k)
                value (pack-tuple v)]
            (.set tr key value)))
        key-values))

(defn -get [tr key]
  (->> key
       (pack-tuple)
       (.get tr)
       (.join)))

(defn -clear [tr key]
  (->> key
       (pack-tuple)
       (.clear tr)))

(defn pr [x]
  (println x)
  x)

(defn bytes-to-string [bytes]
  (-> bytes
      (Tuple/fromBytes)
      (pr)
      (.getString 0)))

