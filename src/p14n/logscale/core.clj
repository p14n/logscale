(ns p14n.logscale.core
  (:require [datascript.core :as d]
            [p14n.logscale.fdb :as fdb]
            [p14n.logscale.storage :as s]))

(defn with-database
  ([f opts]
   (with-open [db (fdb/open-db)]
     (with-database f opts db)))
  ([f {:keys [schema datascript-opts] :as opts} fdb]
   (fdb/transact!
    fdb
    (fn [transaction]
      (let [txs (s/make-tx-storage transaction opts)
            conn (or (d/restore-conn txs datascript-opts)
                     (d/create-conn schema datascript-opts))
            res (f conn)]
        (d/store (d/db conn) txs)
        res)))))

(defn create-db-function [{:keys [schema datascript-opts freeze-fn thaw-fn] :as opts}]
  (fn [f]
    (with-database f opts)))