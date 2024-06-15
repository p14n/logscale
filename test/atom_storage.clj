(ns atom-storage
  (:require [datascript.storage :as ds]))

(defn make-storage [atom-map]
  (reify ds/IStorage
    (-store [_ addr+data-seq]
      (println "Store keys " (map first addr+data-seq))
      (doseq [[addr data] addr+data-seq]
        (when (not (= 1 addr))
          (swap! atom-map assoc addr data))))
    (-restore [_ addr]
      (println "Restore key " addr)
      (get @atom-map addr))))
