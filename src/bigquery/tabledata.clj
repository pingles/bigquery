(ns bigquery.tabledata
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tabledata]
           [com.google.api.services.bigquery.model Table TableDataInsertAllRequest])
  (:require [schema.core :as s]))

(defn insert-all [^Bigquery service project-id dataset-id table-id row]
  (let [insert-request (.setRows (TableDataInsertAllRequest. ) [row])
        op (-> service
               (.tabledata)
               (.insertAll project-id dataset-id table-id insert-request))]
    (.execute op)))
