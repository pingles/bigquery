(ns bigquery.tabledata
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tabledata]
           [com.google.api.services.bigquery.model Table TableDataInsertAllRequest TableDataInsertAllRequest$Rows])
  (:require [schema.core :as s]))

(defn insert-all [^Bigquery service project-id dataset-id table-id row]
  (let [table-data-row (.setJson (TableDataInsertAllRequest$Rows. ) row)
        insert-request (.setRows (TableDataInsertAllRequest. ) [table-data-row])
        op (-> service
               (.tabledata)
               (.insertAll project-id dataset-id table-id insert-request))]
    (.execute op)))
