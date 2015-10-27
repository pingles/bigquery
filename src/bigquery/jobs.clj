(ns bigquery.jobs
  (:use [bigquery.coerce :as bc])
  (:import [java.util Date]
           [com.google.api.services.bigquery.model JobList Job JobConfigurationLoad JobConfigurationQuery JobConfiguration JobStatus JobStatistics JobList$Jobs JobReference GetQueryResultsResponse TableRow TableCell TableSchema TableFieldSchema]))

(extend-protocol bc/ToClojure
  JobReference
  (to-clojure [ref] {:project-id (.getProjectId ref)
                     :job-id     (.getJobId ref)})

  JobList$Jobs
  (to-clojure [job] {:status (to-clojure (.getStatus job))
                     :job-reference (to-clojure (.getJobReference job))
                     :statistics (to-clojure (.getStatistics job))})

  JobStatus
  (to-clojure [status] {:state  (.getState status)
                        :errors (.getErrors status)})
  JobStatistics
  (to-clojure [statistics] {:started (Date. (.getStartTime statistics))
                            :ended   (when-let [e (.getEndTime   statistics)]
                                       (Date. e))})
  Job
  (to-clojure [job] {:job-reference (to-clojure (.getJobReference job))
                     :status        (to-clojure (.getStatus job))
                     :statistics    (to-clojure (.getStatistics job))})
  JobList
  (to-clojure [list]
    (map to-clojure (.getJobs list))))

(defn list [service project-id]
  (let [op (-> service (.jobs) (.list project-id))]
    (to-clojure (.execute op))))

(defn insert [service project-id job]
  (let [op (-> service (.jobs) (.insert project-id job))]
    (to-clojure (.execute op))))


(def query-priority {:interactive "INTERACTIVE"
                     :batch       "BATCH"})

(defn query-job [query-statement & {:keys [use-cache priority flatten allow-large]
                                    :or   {use-cache   false
                                           priority    :interactive
                                           flatten     true
                                           allow-large false}}]
  (let [query (JobConfigurationQuery. )]
    (.setUseQueryCache     query use-cache)
    (.setPriority          query (query-priority priority))
    (.setFlattenResults    query flatten)
    (.setAllowLargeResults query allow-large)
    (.setQuery             query query-statement)
    (doto (Job.)
      (.setConfiguration (-> (JobConfiguration. ) (.setQuery query))))))


(extend-protocol bc/ToClojure
  TableCell
  (to-clojure [cell] (.getV cell))
  TableRow
  (to-clojure [row] (map to-clojure (.getF row)))
  TableSchema
  (to-clojure [schema] (map to-clojure (.getFields schema)))
  TableFieldSchema
  (to-clojure [schema] {:name   (.getName schema)
                        :type   (.getType schema)
                        :fields (seq (map to-clojure (.getFields schema)))
                        :mode   (.getMode schema)}))

(defn query-results [service project-id job-id]
  (letfn [(mk-results-op
            ([] (-> service (.jobs) (.getQueryResults project-id job-id)))
            ([token] (doto (mk-results-op)
                       (.setPageToken token))))]
    (loop [rows   nil
           op     (mk-results-op)]
      (let [result   (.execute op)
            token    (.getPageToken result)
            new-rows (.getRows result)]
        (if (nil? token)
          (let [rows (map to-clojure (concat rows new-rows))]
            {:rows      rows
             :schema    (to-clojure (.getSchema result))
             :bytes     (.getTotalBytesProcessed result)
             :cache-hit (.getCacheHit result)})
          (recur (concat rows new-rows) (mk-results-op token)))))))
