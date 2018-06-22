(ns data-frame.core
  (:gen-class)
  (:require [clojure.pprint :as pprint]
            [clojure.set :as st]
            [clojure.core.matrix :as matrix]
            [flatland.ordered.map :refer [ordered-map]]
            [data-frame.schema :as sch]
            [data-frame.column :as col]
            [data-frame.data-frame :as dframe]
            [data-frame.utils :as u]))


;; X to DataFrame

(defn col-map->DataFrame
  "Creates a DataFrame from a hash-map where each key is the column name and
  each value is a indexed collection contianing elements of a single type and
  nils."
  [col-map]
    (let [col-names (keys col-map)
          col-types (map #(first (u/coll-types (% col-map))) col-names)
          columns (loop [remaining-col-names col-names
                         cols {}]
                    (if (empty? remaining-col-names)
                      cols
                      (recur (rest remaining-col-names)
                             (let [c-name (first remaining-col-names)]
                               (assoc cols c-name
                                      (col/create-column (c-name col-map)))))))]
      (dframe/->DataFrame columns
                          (apply sch/schema
                                 (flatten (seq (zipmap col-names
                                                       col-types)))))))


(defn row-maps->DataFrame
  "Creates a DataFrame from a collection of hash maps each representing a row.
  The set of keys found in each hash-map (row) are used to determine the column
  names of the resulting DataFrame."
  [row-maps]
  (col-map->DataFrame (u/rows-to-columns row-maps)))


; (defn col-vec->DataFrame
;   "Creates a DataFrame from a vector of vectors, each corresponding to a
;   column in the resulting DataFrame. The function also requires a collection
;   of keywords to be used as column names."
;   [col-vecs column-names])
;
;
; (defn row-vecs->DataFrame
;   "Creates a DataFrame from a vector of vectors, each corresponding to a
;   row in the resulting DataFrame. The function also requires a collection
;   of keywords to be used as column names."
;   [row-vecs column-names])


(defn create-data-frame
  "Creates a dataframe."
  [col-name col-data & args]
  (col-map->DataFrame (apply ordered-map col-name col-data args)))


;; DataFrame to X

(defn DataFrame->matrix
  "Creates a vectorz matrix out of a dataframe. All values must be numeric."
  ([df]
    (DataFrame->matrix df :vectorz))
  ([df implemenation]
    (matrix/array implemenation
                  (vec (map #(vec (vals %))
                            (seq df))))))


(defn column-names
  "Returns the names of all the column in the given data-frame."
  [df]
  (keys (.schema df)))


;; Indexing

(defn get-col
  "Returns an entire column of a DataFrame based on the given column name."
  [df col-name]
  (get df col-name))


(defn get-row
  "Returns an entire row of a DataFrame based on the given index."
  [df row-ndx]
  (nth df row-ndx))


;; Size

(defn n-row
  "Returns the number of rows in the given DataFrame."
  [df]
  (count df))


(defn n-col
  [df]
  "Returns the number of columns in the given DataFrame."
  (count (.schema df)))


;; Slicing

(defn row-range
  "Returns a DataFrame of a subset of rows based on a range of indexes.
  Start is inclusive, and end is exclusive."
  [df start end]
  (if (and (zero? start)
           (>= end (n-row df)))
    df
    (let [columns (ordered-map
                    (map (fn [[k v]]
                           [k (col/create-column (subvec v start end))])
                           (.columns df)))]
      (dframe/->DataFrame columns
                          (.schema df)))))


(defn head
  "Returns a DataFrame the first n rows of a DataFrame."
  [df n]
  (row-range df 0 (min n (n-row df))))


(defn tail
  "Returns a DataFrame the last n rows of a DataFrame."
  [df n]
  (row-range df
             (max 0 (- (n-row df) n))
             (n-row df)))


;; Printing

(defn show
  "Prins the firs n (default 50) rows of a DataFrame to stdout. Formats the
  DataFrame as a table."
  ([df]
    (show df 50))
  ([df n]
    (-> (head df n)
        seq
        pprint/print-table)))


(defn print-schema
 "Prints the schema of a DataFrame to stdout. This includes column names
 and their types."
 [df]
 (sch/print-schema (.schema df)))


 ;; Subsetting

(defn select
  [df col-name & args]
  (let [all-col-names (cons col-name args)
        new-columns (select-keys (.columns df) all-col-names)
        new-schema (ordered-map (select-keys (.schema df) all-col-names))]
    (dframe/->DataFrame new-columns new-schema)))


(defn drop-cols
  [df col-name & args]
  (let [col-names (set (column-names df))
        keep-col-names (st/difference col-names
                                      (set (cons col-name args)))]
    (apply select df keep-col-names)))


(defn where
  [df pred]
  (->> (seq df)
       (filter pred)
       row-maps->DataFrame))


(defn unique
  [df]
  (-> (seq df) distinct row-maps->DataFrame))


;; Adding Data

(defn with-column
  [df col-name coll]
  (assoc df col-name coll))


(defn with-column-renamed
  [df old-col-name new-col-name]
  (let [kmap {old-col-name new-col-name}]
    (dframe/->DataFrame (st/rename-keys (.columns df) kmap)
                        (st/rename-keys (.schema df) kmap))))


(defn append-row
  [df row]
  (conj df row))


(defn union
  [df1 df2]
  ())


(defn vertical-stack
  [df1 df2]
  ())


(defn horizontal-stack
  [df1 df2]
  ())


;; Ordering

(defn order-by
  [df by]
  (->> (seq df)
       (sort-by by)
       vec
       row-maps->DataFrame))


;; Joining

; (defn- inner-join-row
;   [row df by])
;
;
; (defn- left-join-row
;   [row df by])
;
;
; (defn- right-join-row
;   [row df by])
;
;
; (defn- anti-join-row
;   [row df2 by])
;
;
; (defn join
;   ([df1 df2 by]
;     (join df1 df2 by :inner))
;   ([df1 df2 by type]
;     (loop [])))


;; Grouping
; See `index` in clojure.set


;;
;; SCRATCH PAD
;;

; (def sch (sch/schema :email String :age Long))
;
; (def email-col
;   (col/create-column ["alice@hmail.com" "cathy@zmail.com" "bob@ymail.com" "eddie@hmail.com"]
;                      String))


; (def age-col
;  (col/create-column [20 45 32 24] Long))


; (defn -main
;   [& args]
;   (-> [{:a 1 :b 2} {:a 3 :b 4}]
;       u/rows-to-columns
;       println))


; (defn -main
;   [& args]
;   (println (type (col/create-column (subvec age-col 1 3)
;                               (.dtype age-col)))))


; (defn -main
;   [& args]
;   (let [df (create-data-frame :email ["alice@hmail.com" "cathy@zmail.com" "bob@ymail.com" "eddie@hmail.com" "dexter@tmail.net"]
;                               :age [20 45 32 24 8]
;                               :is-customer [true false false true true])
;         df (select df :is-customer :email)]
;     (print-schema df)
;     (show df)))
