(ns rica.utils-test
  (:require [clojure.test :refer :all]
            [rica.utils :refer :all]))


(deftest coll-is-of-type
  (testing "Confirm all elements are of type"
    (is (coll-of-type? [1 2 3 nil 5] Long))))


(deftest coll-not-of-type
  (testing "Not all elements are of type"
    (is (not (coll-of-type? #{1 "two" 3 nil 5} Long)))))


(deftest empty-col-of-type
  (testing "All elements of empty coll are of type"
    (is (coll-of-type? '() String))))


(deftest nil-col-of-type
  (testing "Coll of nils is of type Boolean"
    (is (coll-of-type? '(nil nil nil) Boolean))))


(deftest keyword-to-str-stndrd
  (testing "Standard call to keyword-to-str"
    (is (= (keyword-to-str :foo)
           "foo"))))


(deftest coll-types-stndrd
  (testing "Return all types in collection"
    (is (= (coll-types '(1 "two" 3.4 5))
           #{Long String Double}))))


(deftest empty-coll-types
  (testing "Return all types in empty collection"
    (is (= (coll-types []) #{}))))


(deftest coll-of-nils-types
  (testing "Return all types in collection of nils"
    (is (= (coll-types [nil nil nil]) #{}))))


(deftest rows-to-column-vecs-standard
  (testing "Return all types in collection of nils"
    (is (= (rows-to-column-vecs [{:a 1 :b 2} {:a 3 :b 4}])
           {:b [2 4] :a [1 3]}))))


(deftest empty-rows-to-column-vecs
  (testing "Return all types in collection of nils"
    (is (= (rows-to-column-vecs []) {}))))


(deftest index-bag-test
  (testing "Index into bags"
    (is (= (index-bag [{:a 1 :b 2 :c 3}
                       {:a 4 :b 5 :c 6}
                       {:a 1 :b 1 :c 1}
                       {:a 1 :b 7 :c 3}]
                      [:a :c])
           {{:a 1, :c 3} [{:a 1, :b 2, :c 3} {:a 1, :b 7, :c 3}],
            {:a 4, :c 6} [{:a 4, :b 5, :c 6}],
            {:a 1, :c 1} [{:a 1, :b 1, :c 1}]}))))
