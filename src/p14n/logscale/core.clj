(ns p14n.logscale.core
  (:require [datascript.core :as d]
            [p14n.logscale.fdb :as fdb]
            [p14n.logscale.storage :as s]))

(defn with-transaction
  ([f]
   (with-open [db (fdb/open-db)]
     (with-transaction f db)))
  ([f fdb]
   (fdb/transact!
    fdb
    #(f %))))

(defn with-connection
  ([f opts]
   (fn [transaction]
     (with-connection f opts transaction)))
  ([f {:keys [schema datascript-opts] :as opts} transaction]
   (let [txs (s/make-tx-storage transaction opts)
         conn (or (d/restore-conn txs datascript-opts)
                  (d/create-conn schema datascript-opts))
         res (f conn)]
     (d/store (d/db conn) txs)
     res)))

(defn create-datascript-function [{:keys [schema datascript-opts freeze-fn thaw-fn root-keys] :as opts}]
  (fn [f]
    (-> (with-connection f opts)
        (with-transaction))))
