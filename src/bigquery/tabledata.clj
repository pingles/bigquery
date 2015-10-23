(ns bigquery.tabledata
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tabledata]
           [com.google.api.services.bigquery.model Table TableDataInsertAllRequest TableDataInsertAllRequest$Rows TableRow])
  (:require [schema.core :as s]))

(defmulti mk-table-row (fn [row]
                         (cond (map? row)  :record
                               (coll? row) :repeated
                               :else       :value)))
(defmethod mk-table-row :record [m]
  (let [table-row (TableRow. )]
    (doseq [[k v] m]
      (.set table-row k (mk-table-row v)))
    table-row))
(defmethod mk-table-row :repeated [coll]
  (map mk-table-row coll))
(defmethod mk-table-row :value [x]
  x)

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
