(ns rica.core-test
  (:require [clojure.test :refer :all]
            [rica.core :refer :all]
            [rica.data-frame :as dframe]
            [rica.column :refer [create-column]]
            [rica.schema :refer [schema]]))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup

(def df1
  (dframe/->DataFrame {:a (create-column [1 2 3] Long)
                       :b (create-column ["x" "y" "z"] String)}
                      (schema :a Long :b String)))

(def df2
  (dframe/->DataFrame {:b (create-column ["z" nil "x"] String)
                       :c (create-column [false true false] Boolean)}
                      (schema :b String :c Boolean)))

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(deftest df-from-col-map
  (testing "Create data-frame from map of columns."
    (is (= (col-map->DataFrame {:a [1 2 3]
                                :b ["x" "y" "z"]})
           df1))))


(deftest df-from-row-maps
  (testing "Create data-frame from vector of rows."
    (let [df (row-maps->DataFrame [{:a 1 :b "x"}
                                   {:a 2 :b "y"}
                                   {:a 3 :b "z"}])]
      (is (= (.columns df) (.columns df1)))
      (is (= (.schema df) (.schema df1))))))


(deftest create-data-frame-standard
  (testing "Create data-frame."
    (is (= (create-data-frame :a [1 2 3]
                              :b ["x" "y" "z"])
           df1))))


(deftest data-frame-column-names
  (testing "Get column names of data-frame."
    (is (= (column-names df1)
           '(:a :b)))))


(deftest get-data-frame-column
  (testing "Get column of data-frame."
    (is (= (get-col df1 :a)
           (create-column [1 2 3] Long)))))


(deftest get-data-frame-row
  (testing "Get row of data-frame."
    (is (= (get-row df1 1)
           {:a 2 :b "y"}))))


(deftest count-of-df-rows
  (testing "Get count of rows in data-frame."
    (is (= (n-row df1) 3))))


(deftest count-of-df-cols
  (testing "Get count of cols in data-frame."
    (is (= (n-col df1) 2))))


(deftest shape-of-df
  (testing "Get shape of data-frame."
    (is (= (shape df1) (list 3 2)))))


(deftest head-of-df
  (testing "Get head of data-frame."
    (is (= (head df1 2)
           (dframe/->DataFrame {:a (create-column [1 2] Long)
                                :b (create-column ["x" "y"] String)}
                               (schema :a Long :b String))))))


(deftest tail-of-df
  (testing "Get tail of data-frame."
    (is (= (tail df1 2)
           (dframe/->DataFrame {:a (create-column [2 3] Long)
                                :b (create-column ["y" "z"] String)}
                               (schema :a Long :b String))))))

;; select
;; drop-cols

(deftest where-df
  (testing "Filter data-frame based on predicate."
    (is (= (where df1 #(> (:a %) 1))
           (dframe/->DataFrame {:a (create-column [2 3] Long)
                                :b (create-column ["y" "z"] String)}
                               (schema :a Long :b String))))))

;; unique
;; sample
;; with-columns
;; with-column-renamed
;; append-row
;; union
;; vertical-stack
;; horizontal-stack


(deftest order-by-df-desc
  (testing "Ordering data-frame by desc"
    (is (= (order-by df1 [:-a])
           (dframe/->DataFrame {:a (create-column [3 2 1] Long)
                                :b (create-column ["z" "y" "x"] String)}
                               (schema :a Long :b String))))))


(deftest order-by-df-with-nil
  (testing "Ordering data-frame by col with nil"
    (is (= (order-by df2 [:b :c])
           (dframe/->DataFrame {:b (create-column [nil "x" "z"] String)
                                :c (create-column [true false false] Boolean)}
                               (schema :b String :c Boolean))))))


(deftest group-agg-df
  (testing "Group df and aggregate columns"
    (is (= (-> (dframe/->DataFrame {:a (create-column [1 4 1 1] Long)
                                    :b (create-column [2 5 1 7] Long)
                                    :c (create-column [3 6 1 3] Long)}
                                   (schema :a Long :b Long :c Long))
               (group-agg [:a :c] {:cntd #(long (count (distinct %)))}))
           (dframe/->DataFrame {:a (create-column [1 4 1] Long)
                                :c (create-column [3 6 1] Long)
                                :cntd (create-column [2 1 1] Long)}
                               (schema :a Long :c Long :cntd Long))))))


 (deftest just-group-df
   (testing "Group df with no aggregation"
     (is (= (-> (dframe/->DataFrame {:a (create-column [1 4 1 1] Long)
                                     :b (create-column [2 5 1 7] Long)
                                     :c (create-column [3 6 1 3] Long)}
                                    (schema :a Long :b Long :c Long))
                (group-agg [:a :c] {}))
            (dframe/->DataFrame {:a (create-column [1 4 1] Long)
                                 :c (create-column [3 6 1] Long)}
                                (schema :a Long :c Long))))))
