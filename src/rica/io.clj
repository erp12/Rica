(ns rica.io
  "The io namespace contains public functions for reading and writing DataFrames
  to and from data files and other data stores."
  (:require [clojure.java.io :refer [reader writer]]
            [clojure.data.csv :refer [read-csv write-csv]]
            [rica.core :as df]
            [rica.utils :as u]))


(defn from-csv
  "Creates a DataFrame from a csv file. Header is a boolean indicating if the
  file contains a header. If header is false, simple column names will be
  generated.

  Valid options are the same as clojure.data.csv:
     :separator (default \\,)
     :quote (default \\\")"
  [filename header & options]
  (with-open [r (reader filename)]
    (let [csv-contents (apply read-csv r options)]
      (if header
        (df/row-vecs->DataFrame (rest csv-contents)
                                (map keyword (first csv-contents)))
        (df/row-vecs->DataFrame csv-contents
                                (map #(keyword (str "C" %))
                                     (range (count (first csv-contents)))))))))


(defn to-csv
  "Creates a csv file from a DataFrame. Header is a boolean indicating if the
  first line of the file should be column names.

  Valid options are the same as clojure.data.csv:
     :separator (Default \\,)
     :quote (Default \\\")
     :quote? (A predicate function which determines if a string should be quoted. Defaults to quoting only when necessary.)
     :newline (:lf (default) or :cr+lf)"
  [df filename header & options]
  (with-open [w (writer filename)]
    (let [row-vectors (map #(vec (vals %)) (seq df))
          csv-data (if header
                     (cons (vec (map u/keyword-to-str (vec (df/column-names df))))
                           row-vectors)
                     row-vectors)]
      (apply write-csv w csv-data options))))
