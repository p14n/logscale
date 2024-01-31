(ns p14n.logscale.core
  (:require [datascript.core :as d]
            [datascript.storage :as ds]))


(def schema {:aka {:db/cardinality :db.cardinality/many}})
(def opts {:branching-factor 16})

(defonce backing (atom {}))
(defn reset-backing [] (reset! backing {}))

(def storage
  (reify ds/IStorage
    (-store [_ addr+data-seq]
      (println "Store keys " (map first addr+data-seq))
      (doseq [[addr data] addr+data-seq]
        ;(println "Stor ing" addr data)
        (when (not (= 1 addr))
          (swap! backing assoc addr data))))
    (-restore [_ addr]
      (println "Restore key " addr)
      (get @backing addr))))


(defn add-person [conn name age aka]
  (d/transact! conn [{:db/id -1
                      :name  name
                      :age   age
                      :aka   [aka]}]))

(defn update-person [conn id age]
  (d/transact! conn [{:db/id id
                      :age   age}]))

(defn get-person [db aka]
  (d/q `[:find  ?n ?a ?e
         :where [?e :aka ~aka]
         [?e :name ?n]
         [?e :age  ?a]]
       db))

(def conn (d/create-conn schema opts))

(ds/store @conn storage)
;; db is now index roots and meta:
;; {1000038 {:level 0, :keys []},
;;  1000039 {:level 0, :keys []},
;;  1000040 {:level 0, :keys []},
;;  0
;;  {:schema {:aka #:db{:cardinality :db.cardinality/many}},
;;   :max-tx 536870912,
;;   :aevt 1000039,
;;   :max-addr 1000040,
;;   :avet 1000040,
;;   :branching-factor 16,
;;   :max-eid 0,
;;   :ref-type :soft,
;;   :eavt 1000038},
;;  1 []}

(add-person conn "Dean"  30 "Dean")
;; store is called with addr 1 - the first entity datoms
;(ds/store @conn2 storage)
;; store is called with addr 0 (meta) 1 (datoms) eavt, aevt

;(add-person conn2 "Maksim" 45 "Max Otto von Stierlitz")
(add-person conn "Mak_sim" 454 "Min Otto von Stierlitz")

(get-person (d/db conn) "Dean")

(def conn2 (d/restore-conn storage (assoc opts :schema schema)))
;(d/listen! conn2 #(println "tx " %))
;(ds/restore @conn)
;(d/collect-garbage conn)
@conn2
(reset-backing)
@backing
(keys @backing)

(get @backing 0)
(count (second (get @backing 1)))

(defn do-store [cn]
  (ds/store @cn storage)
  (get @backing 0))

(update-person conn 8 31)
(do-store conn2)