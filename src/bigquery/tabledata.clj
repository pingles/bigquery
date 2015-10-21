(ns bigquery.tabledata
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tabledata]
           [com.google.api.services.bigquery.model Table TableDataInsertAllRequest TableDataInsertAllRequest$Rows TableRow])
  (:require [schema.core :as s]))


(defn- mk-table-row [row]
  (let [table-row (TableRow.)]
    (doseq [keyval (seq row)]
      (.set table-row (key keyval) (val keyval)))

    table-row))

(defn insert-all [^Bigquery service project-id dataset-id table-id row]
  (let [row            ( mk-table-row row)
        table-data-row (.setJson (TableDataInsertAllRequest$Rows. ) row)
        insert-request (.setRows (TableDataInsertAllRequest. ) [table-data-row])
        op (-> service
               (.tabledata)
               (.insertAll project-id dataset-id table-id insert-request))]
    (.execute op)))
