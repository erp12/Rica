(ns data-frame.data-frame-test
  (:require [clojure.test :refer :all]
            [flatland.ordered.map :refer [ordered-map]]
            [data-frame.data-frame :refer :all]
            [data-frame.column :refer [create-column]]
            [data-frame.schema :refer [schema]]))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup

(def sch1 (schema :name String :age Long))

(def cols1 {:name (create-column ["Alice" "Bob" "Eddie"] String)
            :age (create-column [32 45 24] Long)})

(def df1 (->DataFrame cols1 sch1))

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(deftest df-does-contain-key
  (testing "Confirm dataframe contains key"
    (is (contains? df1 :name))))


(deftest df-does-not-contain-key
  (testing "Confirm dataframe doesn't contain key"
    (is (not (contains? df1 :foo)))))


(deftest assoc-new-column-in-df
  (testing "Assoc a new column into data-frame"
    (is (= (assoc df1 :is-customer
                  (create-column [false true false] Boolean))
           (->DataFrame {:name (create-column ["Alice" "Bob" "Eddie"] String)
                         :age (create-column [32 45 24] Long)
                         :is-customer (create-column [false true false] Boolean)}
                        (schema :name String :age Long :is-customer Boolean))))))


(deftest assoc-existing-column-in-df
  (testing "Assoc a existing column into data-frame"
    (is (= (assoc df1 :age (create-column [1 2 3] Long))
           (->DataFrame {:name (create-column ["Alice" "Bob" "Eddie"] String)
                         :age (create-column [1 2 3] Long)}
                        sch1)))))


(deftest assoc-bad-column-in-df
  (testing "Assoc a existing column into data-frame"
    (is (thrown? Exception (assoc df1 :age (create-column [1 2 3 4] Long))))))


(deftest get-column-in-df
  (testing "Get a column in a DataFrame"
    (is (= (get df1 :age) (create-column [32 45 24] Long)))))


(deftest get-nonexistant-column-in-df
  (testing "Get a column in a DataFrame"
    (is (nil? (get df1 :foo)))))


(deftest get-nonexistant-column-in-df
  (testing "Get a column in a DataFrame"
    (is (= (get df1 :foo :NOT-FOUND)
           :NOT-FOUND))))


(deftest conj-row-map-into-df
  (testing "Add a new row from a map."
    (is (= (conj df1 {:age 1 :name "Foo"})
           (->DataFrame {:name (create-column ["Alice" "Bob" "Eddie" "Foo"] String)
                         :age (create-column [32 45 24 1] Long)}
                        sch1)))))


(deftest conj-nonconforming-row-map-into-df
  (testing "Attempt to add a bad row from a map."
    (is (thrown? Exception (conj df1 {:age true :name "Foo"})))))


(deftest conj-row-vec-into-df
  (testing "Add a new row from a vector."
    (is (= (conj df1 ["Foo" 1])
           (->DataFrame {:name (create-column ["Alice" "Bob" "Eddie" "Foo"] String)
                         :age (create-column [32 45 24 1] Long)}
                        sch1)))))


(deftest conj-nonconforming-row-vec-into-df
  (testing "Attempt to add a bad row from a vector."
    (is (thrown? Exception (conj df1 [1 "Foo"])))))


(deftest empty-df
  (testing "Empty a DataFrame."
    (is (= (empty df1)
           (->DataFrame {:name (create-column [] String)
                         :age (create-column [] Long)}
                        sch1)))))


(deftest df-to-seq
  (testing "Dataframe to seq of rows."
    (is (= (seq df1)
           (list (ordered-map :name "Alice" :age 32)
                 (ordered-map :name "Bob" :age 45)
                 (ordered-map :name "Eddie" :age 24))))))


(deftest empy-df-to-seq
  (testing "Dataframe to seq of rows."
    (is (nil? (seq (empty df1))))))


(deftest nth-row-in-df
  (testing "Get a row of a DataFrame."
    (is (= (nth df1 1)
           {:name "Bob" :age 45}))))


(deftest nth-column-in-df-oob
  (testing "Get an out of bounds row of a DataFrame."
    (is (nil? (nth df1 100)))))


(deftest nth-row-of-empty-df
  (testing "Get row of empty DataFrame."
    (is (= (nth (empty df1) 0 :NOT-FOUND)
           :NOT-FOUND))))


(deftest count-rows-in-df
  (testing "Get count of DataFrame rows."
    (is (= 3 (count df1)))))


(deftest count-rows-in-empty-df
  (testing "Get count of empty DataFrame rows."
    (is (= 0 (count (empty df1))))))
