(ns bigquery.tables
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Tables]
           [com.google.api.services.bigquery.model Table TableList$Tables TableReference])
  (:require [bigquery.coerce :as bc]))

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
