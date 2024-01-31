(ns p14n.logscale.storage
  (:require [datascript.storage :as ds]
            [p14n.logscale.fdb :as fdb]))

(defn make-storage [db]
  (reify ds/IStorage
    (-store [_ addr+data-seq]
      (println "Store keys " (map first addr+data-seq))
      (fdb/transact!
       db
       #(fdb/set! %
                  (->> addr+data-seq
                       (remove (fn [[addr _]] (= 1 addr)))))))
    (-restore [_ addr]
      (println "Restore key " addr)
      (fdb/transact!
       db
       #(fdb/-get % addr)))))
