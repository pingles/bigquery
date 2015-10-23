(ns bigquery.tabledata-test
  (:require [bigquery.tabledata :refer :all]
            [clojure.test :refer :all]))

(deftest table-row
  (let [r (mk-table-row {"test" "name"})]
    (is (= "name" (get r "test"))))
  (let [r (mk-table-row "value")]
    (is (= "value" r)))
  (let [r (mk-table-row ["a" "value"])]
    (is (= ["a" "value"] r)))
  (let [r (mk-table-row {"person" {"name" "paul"}})]
    (is (= "paul" (get-in r ["person" "name"]))))
  (let [r (mk-table-row {"aliases" ["paul" "pingles"]})]
    (is (= ["paul" "pingles"] (get r "aliases")))))
