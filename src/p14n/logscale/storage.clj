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
  [tr {:keys [freeze-fn thaw-fn root-keys]
       :or {freeze-fn default-freeze-fn
            thaw-fn default-thaw-fn}}]
  (when (nil? root-keys)
    (throw (ex-info "Root-keys must be set" {})))
  (reify ds/IStorage
    (-store [_ addr+data-seq]
      (fdb/-set! tr
                 (->> addr+data-seq
                      (remove (fn [[addr _]] (= 1 addr)))
                      (map (fn [[addr data]]
                             [(conj root-keys addr) (freeze-fn data)])))))
    (-restore [_ addr]
      (thaw-fn (fdb/-get tr (conj root-keys addr))))))
