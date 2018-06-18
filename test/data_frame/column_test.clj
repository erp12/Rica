(ns data-frame.column-test
  (:require [clojure.test :refer :all]
            [data-frame.column :refer :all]))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup

(def int-column
  (create-column (range 5) Long))


(def email-column
  (create-column ["alice@hmail.com" "cathy@zmail.com" "bob@ymail.com" "eddie@hmail.com"]
                 String))


(def nil-column
  (create-column (repeat 10 nil)))


(def empty-column
  (create-column [] Boolean))

;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

;; Seq

(deftest test-seq-column
  (testing "Turn column into seq"
    (is (= (seq email-column)
           '("alice@hmail.com" "cathy@zmail.com" "bob@ymail.com" "eddie@hmail.com")))))


(deftest test-seq-column-nils
  (testing "Turn column into seq (nils)"
    (is (= (seq nil-column)
           (repeat 10 nil)))))


(deftest test-seq-column-empty
  (testing "Turn column into seq (empty)"
    (is (= (seq empty-column) nil))))

;; Count

(deftest test-count-column
  (testing "Count number of elements in column"
    (is (= (count int-column) 5))))


(deftest test-count-column-nils
  (testing "Count number of elements in column (nils)"
    (is (= (count nil-column) 10))))


(deftest test-count-column-empty
  (testing "Count number of elements in column (empty)"
    (is (= (count empty-column) 0))))

;; Conj

(deftest test-conj-column
  (testing "Conj number to column"
    (is (= (conj int-column 100)
           (create-column [0 1 2 3 4 100] java.lang.Long)))))


(deftest test-conj-column-nils
  (testing "Conj nil to column"
    (is (= (conj int-column nil)
           (create-column [0 1 2 3 4 nil] java.lang.Long)))))


(deftest test-conj-column-wrong-type
  (testing "Conf number to boolean column"
    (is (thrown? Exception (conj empty-column 7)))))

;; Empty

(deftest test-empty-column
  (testing "Empty a column"
    (is (empty email-column)
        (create-column [] java.lang.String))))

;; Empty

(deftest test-equiv-column-true
  (testing "Column equivalence (true)"
    (is (= email-column email-column))))


(deftest test-equiv-column-false
  (testing "Column equivalence (false)"
    (is (not= int-column email-column))))


(deftest test-equiv-column-same-type
  (testing "Column equivalence (same type, different data)"
    (is (not= nil-column empty-column))))

;; Assoc

(deftest test-assoc-column
  (testing "Assoc val to column"
    (is (= (assoc int-column 1 100)
           (create-column [0 100 2 3 4] java.lang.Long)))))


(deftest test-assoc-column-nil
  (testing "Assoc nil to column"
    (is (= (assoc email-column 1 nil)
           (create-column ["alice@hmail.com" nil "bob@ymail.com" "eddie@hmail.com"]
                          java.lang.String)))))


(deftest test-assoc-column-empty
  (testing "Assoc val to empty column"
    (is (= (assoc empty-column 0 true)
           (create-column [true] java.lang.Boolean)))))


(deftest test-assoc-column-oob
  (testing "Assoc val to column (out of bounds)"
    (is (thrown? IndexOutOfBoundsException (assoc int-column 50 100)))))

;; Nth

(deftest test-nth-column
  (testing "Nth val in column"
    (is (= (nth email-column 2) "bob@ymail.com"))))


(deftest test-nth-column-oob
  (testing "Nth val in column (out of bounds)"
    (is (thrown? IndexOutOfBoundsException (nth int-column 50)))))

;; First

(deftest test-first-column
  (testing "First val in column"
    (is (= (first email-column) "alice@hmail.com"))))

;; Second

(deftest test-second-column
  (testing "Second val in column"
    (is (= (second int-column) 1))))

;; Last

(deftest test-last-column
  (testing "Last val in column"
    (is (= (last email-column) "eddie@hmail.com"))))

;; Column-type

(deftest test-column-type-int
  (testing "Type of int column"
    (is (= (column-type int-column) java.lang.Long))))


(deftest test-column-type-string
  (testing "Type of int column"
    (is (= (column-type email-column) java.lang.String))))


(deftest test-column-type-boolean
  (testing "Type of int column"
    (is (= (column-type nil-column)
           (column-type empty-column)
           java.lang.Boolean))))
