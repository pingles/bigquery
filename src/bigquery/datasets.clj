(ns bigquery.datasets
  (:import [com.google.api.services.bigquery Bigquery Bigquery$Datasets]
           [com.google.api.services.bigquery.model DatasetList$Datasets DatasetReference]))

(defprotocol ToClojure
  (to-clojure [x]))

(extend-protocol ToClojure
  DatasetReference
  (to-clojure [ref] {:dataset-id (.getDatasetId ref)
                     :project-id (.getProjectId ref)})
  DatasetList$Datasets
  (to-clojure [ds] {:id (.getId ds)
                    :name (.getFriendlyName ds)
                    :reference (to-clojure (.getDatasetReference ds))}))

(defn list [^Bigquery service project-id]
  (let [list-op (-> service (.datasets) (.list project-id))]
    (->> (.execute list-op)
         (.getDatasets)
         (map to-clojure))))
