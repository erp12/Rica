(ns rica.schema-test
  (:require [clojure.test :refer :all]
            [rica.schema :refer :all]
            [flatland.ordered.map :refer [ordered-map]]))


(def sch
  (schema :name String :age Long :foo clojure.lang.PersistentVector))


(def map-data
  {:name "Eddie" :foo [1 2 3] :age 24})

(def vec-data
  ["Eddie" 24 [1 2 3]])


(deftest new-schema
  (testing "Create a new schema."
    (is (= (schema :age Long :name String)
           (ordered-map :age Long :name String)))))


(deftest new-schema-bad-key
  (testing "Create schema with bad key"
    (is (thrown? Exception
                 (schema :age Long "name" String)))))


(deftest new-schema-bad-class
  (testing "Create schema with bad class (value)"
    (is (thrown? Exception
                 (schema :age "String" :name String)))))


(deftest map-conforms-to-schema
  (testing "Map which conforms to schema"
    (is (map-conforms? map-data sch))))


(deftest map-not-conforms-to-schema
  (testing "Map which does not conform to schema"
    (is (not (map-conforms? {:foo "bar"} sch)))))


(deftest vec-conforms-to-schema
  (testing "Vector which conforms to schema"
    (is (vec-conforms? vec-data sch))))


(deftest small-vec-not-conforms-to-schema
  (testing "Vector which does not conform to schema (too small)"
    (is (not (vec-conforms? ["bar" 24] sch)))))


(deftest vec-not-conforms-to-schema
  (testing "Vector which does not conform to schema (wrong types)"
    (is (not (vec-conforms? ["bar" 24.5 [5 4 3]] sch)))))
