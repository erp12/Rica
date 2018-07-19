(ns rica.io-test
  (:require [clojure.test :refer :all]
            [rica.core :refer [create-data-frame]]
            [rica.io :refer :all]))


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup

(def tmp-csv
  (java.io.File/createTempFile "rica-createTempFile" ".csv"))

(with-open [file (clojure.java.io/writer tmp-csv)]
  (binding [*out* file]
    (println "A,B")
    (println "1,x")
    (println "2,y")))

(.deleteOnExit tmp-csv)


;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(deftest from-csv-with-header
  (testing "Create DataFrame from CSV with header"
    (is (= (from-csv (.getAbsolutePath tmp-csv) true)
           (create-data-frame :A ["1" "2"]
                              :B ["x" "y"])))))


(deftest from-csv-without-header
  (testing "Create DataFrame from CSV without header"
    (is (= (from-csv (.getAbsolutePath tmp-csv) false)
           (create-data-frame :C0 ["A" "1" "2"]
                              :C1 ["B" "x" "y"])))))
