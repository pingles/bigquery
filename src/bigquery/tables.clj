(ns bigquery.tables
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tables]
           [com.google.api.services.bigquery.model Table TableList$Tables TableReference])
  (:require [bigquery.coerce :as bc]
            [schema.core :as s]))

(extend-protocol bc/ToClojure
  TableReference
  (to-clojure [ref] {:project-id (.getProjectId ref)
                     :dataset-id (.getDatasetId ref)
                     :table-id   (.getTableId ref)})
  TableList$Tables
  (to-clojure [tables]
    {:id            (.getId tables)
     :friendly-name (.getFriendlyName tables)
     :reference     (bc/to-clojure (.getTableReference tables))}))

(defn list [^Bigquery service project-id dataset-id]
  (letfn [(mk-list-op
            ([] (-> service (.tables) (.list project-id dataset-id)))
            ([page-token] (doto (mk-list-op)
                            (.setPageToken page-token))))]
    (->> (loop [tables  nil
                list-op (mk-list-op)]
           (let [result (.execute list-op)
                 token  (.getNextPageToken result)
                 new-tables (.getTables result)]
             (if (nil? token)
               (concat tables new-tables)
               (recur (concat tables new-tables) (mk-list-op token)))))
         (map bc/to-clojure))))


(def TableReferenceSchema
  "A reference for a table in a dataset"
  {:table-id s/Str
   :project-id s/Str
   :dataset-id s/Str})

(defn- mk-table-reference [{:keys [table-id project-id dataset-id] :as ref}]
  (doto (TableReference. )
    (.setProjectId project-id)
    (.setDatasetId dataset-id)
    (.setTableId   table-id)))

(def TableSchema
  "BigQuery Table schema"
  {:table-reference TableReferenceSchema
   (s/optional-key :description) s/Str})

(defn- mk-table [{:keys [table-reference description] :as table}]
  {:pre [(s/validate TableSchema table)]}
  (doto (Table. )
    (.setTableReference (mk-table-reference table-reference))
    (.setDescription    description)))

(extend-protocol bc/ToClojure
  Table
  (to-clojure [table] {:id (.getId table)
                       :table-reference (bc/to-clojure (.getTableReference table))
                       :friendly-name (.getFriendlyName table)
                       :description   (.getDescription table)}))

(defn insert [^Bigquery service {:keys [table-reference] :as table}]
  (let [op (-> service
               (.tables)
               (.insert (:project-id table-reference) (:dataset-id table-reference) (mk-table table)))]
    (bc/to-clojure (.execute op))))

(defn delete [^Bigquery service project-id dataset-id table-id]
  (let [delete-op (-> service (.tables) (.delete project-id dataset-id table-id))]
    (.execute delete-op)))
