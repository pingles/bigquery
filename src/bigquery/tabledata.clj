(ns bigquery.tabledata
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tabledata]
           [com.google.api.services.bigquery.model Table TableDataInsertAllRequest TableDataInsertAllRequest$Rows TableRow])
  (:require [schema.core :as s]))


(defn- mk-table-row [row]
  (let [table-row (TableRow.)]
    (doseq [[k v] row]
      (if (coll? v)
        (.set table-row k (map mk-table-row v))
        (.set table-row k v)))
    table-row))

(defn- mk-insert-request-row [table-row]
  (doto (TableDataInsertAllRequest$Rows. )
    (.setJson table-row)))

(defn insert-all [^Bigquery service project-id dataset-id table-id rows]
  (let [data (->> rows
                  (map mk-table-row)
                  (map mk-insert-request-row))
        request (doto (TableDataInsertAllRequest. )
                  (.setRows data))
        op      (-> service (.tabledata) (.insertAll project-id dataset-id table-id request))]
    (.execute op)))
