(ns rica.core
  "The primary data-frame API offered by Rica. Includes functions for creating
  and manipulating data-frames."
  (:gen-class)
  (:require [clojure.pprint :as pprint]
            [clojure.set :as st]
            [clojure.core.matrix :as matrix]
            [flatland.ordered.map :refer [ordered-map]]
            [rica.schema :as sch]
            [rica.column :as col]
            [rica.data-frame :as dframe]
            [rica.agg :as a]
            [rica.joins :as j]
            [rica.utils :as u]))


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
  names of the resulting DataFrame.

  WARNING: When going from DataFrame to row-maps back to DataFrame using seq and
  and row-maps->DataFrame the column order may change."
  [row-maps]
  (if (empty? row-maps)
    (dframe/->DataFrame [] {})
    (col-map->DataFrame (u/rows-to-column-vecs row-maps))))


(defn col-vec->DataFrame
  "Creates a DataFrame from a vector of vectors, each corresponding to a
  column in the resulting DataFrame. The function also requires a collection
  of keywords to be used as column names."
  [col-vecs column-names]
  (if (not (every? #(= (count %) (count (first col-vecs))) col-vecs))
     (throw (Exception. "Column lengths do not match.")))
  (let [columns (into {}
                      (zipmap column-names
                              (map col/create-column col-vecs)))
        schema (ordered-map (into {}
                                  (zipmap column-names
                                          (map (fn [[k v]] (col/column-type v))
                                               columns))))]
    (dframe/->DataFrame columns schema)))


(defn row-vecs->DataFrame
  "Creates a DataFrame from a vector of vectors, each corresponding to a
  row in the resulting DataFrame. The function also requires a collection
  of keywords to be used as column names."
  [row-vecs column-names]
  (col-vec->DataFrame (u/transpose-vectors row-vecs)
                      column-names))


(defn create-data-frame
  "Creates a dataframe."
  [col-name col-data & args]
  (col-map->DataFrame (apply ordered-map col-name col-data args)))


(defn empty-data-frame
  "Creates an empty dataframe of the given schema."
  [schema]
  (dframe/->DataFrame [] schema))


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
  "Returns the number of columns in the given DataFrame."
  [df]
  (count (.schema df)))


(defn shape
  "Returns the shape of the given data-frame as a list where the first element
  is the number of rows and the second element is the number of columns."
  [df]
  (list (n-row df) (n-col df)))

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
  (row-range df (max 0 (- (n-row df) n)) (n-row df)))


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


(defn touch
  "Performs a function on a DataFrame (presumably for side effects) and then
  returns the origional DataFrame. Useful for debugging and tracing when
  creating pipelines with the threading macro."
  [df func]
  (do (func df) df))


 ;; Subsetting

(defn select
  "Returns a data-frame with only the given columns present."
  [df col-names]
  (let [new-columns (select-keys (.columns df) col-names)
        new-schema (ordered-map (select-keys (.schema df) col-names))]
    (dframe/->DataFrame new-columns new-schema)))


(defn drop-cols
  "Returns a data-frame with the given columns removed."
  [df col-names]
  (let [current-col-names (set (column-names df))
        keep-col-names (st/difference current-col-names
                                      (set col-names))]
    (select df keep-col-names)))


(defn where
  "Returns a dataframe where all rows match the given predicate."
  [df pred]
  (let [col-order (column-names df)
        result-df (->> (seq df)
                       (filter pred)
                       row-maps->DataFrame)]
    (select result-df col-order)))


(defn unique
  "Returns the given df with only the unique rows."
  [df]
  (-> (seq df) distinct row-maps->DataFrame))


(defn sample
  "Returns a random sample of the given data-frame with a fraction of the rows."
  [df frac]
  (->> (seq df)
       (random-sample frac)
       row-maps->DataFrame))

;; Adding Data

(defn with-column
  "Adds a new column to the given dataframe by associating the given column
  name with the new colllection."
  [df col-name col-expr]
  (let [new-col (map col-expr (seq df))]
    (assoc df col-name new-col)))


(defn with-column-renamed
  "Renames a column in the DataFrame."
  [df old-col-name new-col-name]
  (let [kmap {old-col-name new-col-name}]
    (dframe/->DataFrame (st/rename-keys (.columns df) kmap)
                        (st/rename-keys (.schema df) kmap))))


(defn append-row
  "Append a row (either as a map or vector) to the given data-frame."
  [df row]
  (conj df row))


(defn union
  [df1 df2]
  (loop [remaining-cols (st/union (set (column-names df1))
                                  (set (column-names df2)))
         new-columns-map (.columns df1)]
    (if (empty? remaining-cols)
      (col-map->DataFrame new-columns-map)
      (let [col-name (first remaining-cols)
            df1-col (if (contains? df1 col-name)
                      (get df1 col-name)
                      (vec (repeat (n-row df1) nil)))
            df2-col (if (contains? df2 col-name)
                      (get df2 col-name)
                      (vec (repeat (n-row df2) nil)))
            new-col-vec (vec (concat df1-col df2-col))]
        (recur (rest remaining-cols)
               (assoc new-columns-map col-name new-col-vec))))))


(defn vertical-stack
  [df1 df2]
  (if (not= (n-col df1) (n-col df2))
    (throw (Exception. "Can only vertically stack data-frames with same column counts.")))
  (let [columns (ordered-map (map (fn [[k1 v1] [k2 v2]]
                                    [k1 (col/create-column (concat v1 v1))])
                                  (.columns df1)
                                  (.columns df2)))]
      (dframe/->DataFrame columns (.schema df1))))


(defn horizontal-stack
  [df1 df2]
  (if (not= (n-row df1) (n-row df2))
    (throw (Exception. "Can only horizontally stack data-frames with same row counts.")))
  (let [rows (map merge (seq df1) (seq df2))]
    (row-maps->DataFrame rows)))


;; Ordering

(defn order-by
  "Returns the given data-frame with the rows ordered by one or more columns.
  Column names should be in the `by` vector. To sort by descending order,
  put a `-` as the first character of the keyword denoting the column name.
  (ie. `:price` because `:-price`)"
  [df by]
  (let [col-order (column-names df)
        by-str (map u/keyword-to-str by)
        desc? (map #(= \- (first %)) by-str)
        by-col-names (map #(if (nth desc? %)
                             (keyword (apply str (rest (nth by-str %))))
                             (nth by %))
                          (range (count by)))
        row-comp (fn [r1 r2]
                   (loop [ndx 0
                          remaining1 r1
                          remaining2 r2]
                     (let [x (first remaining1)
                           y (first remaining2)
                           c (if (nth desc? ndx false)
                               (* -1 (compare x y))
                               (compare x y))]
                       (cond
                         (and (empty? remaining1) (empty? remaining2))
                         0

                         (or (empty? remaining1) (empty? remaining2))
                         (compare (count r1) (count r2))

                         (zero? c)
                         (recur (inc ndx)
                                (rest remaining1)
                                (rest remaining2))

                         :else
                         c))))
        result-df (->> (seq df)
                       (sort-by (apply juxt by-col-names) row-comp)
                       vec
                       row-maps->DataFrame)]
    (select result-df col-order)))


;; Joining

(defn join
  ([left-df right-df join-on]
    (join left-df right-df join-on "inner"))
  ([left-df right-df join-on join-type]
    (let [join-fn (j/find-join-fn join-on join-type)
          result-rows (join-fn left-df right-df)]
      (row-maps->DataFrame result-rows))))


(defn cross-join
  [df1 df2]
  (join df1 df2 (fn [l r] true) "cross"))


;; Grouping

(defn agg
  [df agg-exprs]
  (let [rows (seq df)
        agg-row (apply merge
                        (map (fn [[result-name agg-expr]]
                               {result-name (agg-expr rows)})
                             agg-exprs))]
    (row-maps->DataFrame [agg-row])))


(defn group-agg
  [df by agg-exprs]
  (let [groups (u/index-bag (seq df) by)
        bases (keys groups)
        agg-vals (map (fn [group]
                        (apply merge
                               (map (fn [[result-name agg-expr]]
                                      {result-name (agg-expr group)})
                                     agg-exprs)))
                      (vals groups))]
    (-> (map merge bases agg-vals)
        row-maps->DataFrame
        (select (concat (keys (reduce merge bases)) (keys agg-exprs))))))


;;
;; SCRATCH PAD
;;


; (defn -main
;   [& args]
;   (let [df (create-data-frame :foo [1 2 3 2 1]
;                               :bar [5 4 3 2 1]
;                               :baz [1 1 2 2 3])
;         _ (j/create-hash-table df [:foo])]
;     (println _)))


(defn -main
  [& args]
  (let [df1 (create-data-frame :my-int [1 2 2]
                               :my-bool [true true false])
        df2 (create-data-frame :my-int [2 3 3 4]
                               :my-str ["a" "b" "c" "d"])
        _ (join df1 df2 [:my-int])
    ]
    (println "===================================")
    (println _)
    (print-schema _)
    (show _)))
