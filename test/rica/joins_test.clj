(ns rica.joins-test
  (:require [clojure.test :refer :all]
            [flatland.ordered.map :refer [ordered-map]]
            [rica.joins :refer :all]
            [rica.data-frame :as dframe]
            [rica.column :refer [create-column]]
            [rica.schema :refer [schema]]))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup

(def df1
  (dframe/->DataFrame {:i (create-column [1 2 2] Long)
                       :b (create-column [true true false] Boolean)}
                      (schema :i Long :b Boolean)))

(def df2
  (dframe/->DataFrame {:i (create-column [2 3 3 4] Long)
                       :s (create-column ["a" "b" "c" "d"] String)}
                      (schema :i Long :s String)))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(deftest hash-map-test
  (testing "Create hash map from dataframe."
    (is (= (hash-table df2 [:i])
           {{:i 2} [(ordered-map :i 2 :s "a")]
            {:i 3} [(ordered-map :i 3 :s "b") (ordered-map :i 3 :s "c")]
            {:i 4} [(ordered-map :i 4 :s "d")]}))))


(deftest inner-hash-join-test
  (testing "Inner hash join"
    (is (= (hash-join df1 df2 [:i] "inner")
           [{:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}]))))


(deftest left-hash-join-test
  (testing "Left outer hash join"
    (is (= (hash-join df1 df2 [:i] "left_outer")
           [{:i 1 :s nil :b true}
            {:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}]))))


(deftest right-hash-join-test
  (testing "Right outer hash join"
    (is (= (hash-join df1 df2 [:i] "right-outer")
           [{:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}
            {:i 3 :s "b" :b nil}
            {:i 3 :s "c" :b nil}
            {:i 4 :s "d" :b nil}]))))


(deftest full-hash-join-test
  (testing "Full outer hash join"
    (is (= (hash-join df1 df2 [:i] "full")
           [{:i 1 :s nil :b true}
            {:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}
            {:i 3 :s "b" :b nil}
            {:i 3 :s "c" :b nil}
            {:i 4 :s "d" :b nil}]))))


(deftest inner-nested-loop-join-test
  (testing "Inner nested-loop join"
    (is (= (nested-loop-join df1 df2 #(= (:i %1) (:i %2)) "inner")
           [{:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}]))))


(deftest left-nested-loop-join-test
  (testing "Left outer nested-loop join"
    (is (= (nested-loop-join df1 df2 #(= (:i %1) (:i %2)) "left_outer")
           [{:i 1 :s nil :b true}
            {:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}]))))


(deftest right-nested-loop-join-test
  (testing "Right outer nested-loop join"
    (is (= (nested-loop-join df1 df2 #(= (:i %1) (:i %2)) "right-outer")
           [{:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}
            {:i 3 :s "b" :b nil}
            {:i 3 :s "c" :b nil}
            {:i 4 :s "d" :b nil}]))))


(deftest full-nested-loop-join-test
  (testing "Full outer nested-loop join"
    (is (= (nested-loop-join df1 df2 #(= (:i %1) (:i %2)) "full")
           [{:i 1 :s nil :b true}
            {:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}
            {:i 3 :s "b" :b nil}
            {:i 3 :s "c" :b nil}
            {:i 4 :s "d" :b nil}]))))


(deftest nested-loop-cross-join-test
  (testing "Cross join"
    (is (= (nested-loop-join df1 df2 (fn [l r] true) "cross")
           [{:i 1 :s "a" :b true}
            {:i 2 :s "a" :b true}
            {:i 2 :s "a" :b false}
            {:i 1 :s "b" :b true}
            {:i 2 :s "b" :b true}
            {:i 2 :s "b" :b false}
            {:i 1 :s "c" :b true}
            {:i 2 :s "c" :b true}
            {:i 2 :s "c" :b false}
            {:i 1 :s "d" :b true}
            {:i 2 :s "d" :b true}
            {:i 2 :s "d" :b false}]))))
