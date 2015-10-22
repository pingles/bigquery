# bigquery

Clojure client to interact with [Google Cloud's BigQuery](https://cloud.google.com/bigquery) service.

## Usage

### Authenticating

Currently `bigquery` supports service account authentication.

```clojure
(ns example
  (:require [bigquery.auth :as ba]))

(def bigquery-service (ba/service-account "some-account@developer.gserviceaccount.com" "./path/to/creds.p12"))
```

### Creating a Dataset

```clojure
(ns example
  (:require [bigquery.datasets :as bd]))

(bd/insert bigquery-service "project-id" {:id "dataset_id" :friendly-name "name"})
```

### Creating a Table

```clojure
(ns example
  (:require [bigquery.tables :as bt]))
  
(def table {:table-reference {:table-id "table_name"
                              :project-id "project-id"
                              :dataset-id "dataset-id"}
            :description "Description of the table"
            :schema      [{:name "colA"
                           :type :string
                           :mode :required}
                          {:name "addresses"
                           :type :record
                           :mode :repeated
                           :fields [{:name "line1"
                                     :type :string
                                     :mode :required]}}]})

(bt/insert bigquery-service table)
```

### Streaming insert data

```clojure
(ns example
  (:require [bigquery.tabledata :as btd]))

(def sample-data [{"colA"      "Hello, world"
                   "addresses" [{"line1" "Address Here"}
                                {"line1" "New Address Here"}]}])

(btd/insert-all bigquery-service "project-id" "dataset-id" "table-id" sample-data)
```

## License

Copyright &copy; 2015 Paul Ingles

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
