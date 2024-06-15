(ns p14n.logscale.storage
  (:require [datascript.storage :as ds]
            [p14n.logscale.fdb :as fdb]
            [clojure.edn :as edn]))

(defn default-freeze-fn [data]
  (pr-str data))

(defn default-thaw-fn [data]
  (some-> data
          (fdb/bytes-to-string)
          (edn/read-string)))

(defn make-tx-storage
  [tr {:keys [freeze-fn thaw-fn] :or {freeze-fn default-freeze-fn
                                      thaw-fn default-thaw-fn}}]
  (reify ds/IStorage
    (-store [_ addr+data-seq]
      (println "Store keys" (map first addr+data-seq))
      (fdb/-set! tr
                 (->> addr+data-seq
                      (remove (fn [[addr _]] (= 1 addr)))
                      (map (fn [[addr data]] [addr (-> (freeze-fn data))])))))
    (-restore [_ addr]
      (println "Restore key" addr)
      (thaw-fn (fdb/-get tr addr)))))
