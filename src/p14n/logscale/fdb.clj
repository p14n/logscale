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

(defn- pack-tuple [ks]
  (.pack (Tuple/from (into-array Object ks))))

(defn -set! [tr key-values]
  (run! (fn [[ks v]]
          (let [key (pack-tuple ks)
                value (pack-tuple [v])]
            (.set tr key value)))
        key-values))

(defn -get [tr keys]
  (->> keys
       (pack-tuple)
       (.get tr)
       (.join)))

(defn -clear [tr keys]
  (->> keys
       (pack-tuple)
       (.clear tr)))

(defn bytes-to-string [bytes]
  (-> bytes
      (Tuple/fromBytes)
      (.getString 0)))

