(ns rica.utils
  "Utility functions used throughout Rica."
  (:require [flatland.ordered.map :refer [ordered-map]]))


(defn coll-of-type?
  "Returns true if all elements of coll are of type dtype. Returns false otherwise."
  ([coll dtype]
    (coll-of-type? coll dtype true))
  ([coll dtype allow-nil]
    (if allow-nil
      (every? #(or (nil? %) (instance? dtype %)) coll)
      (every? #(instance? dtype %) coll))))


(defn keyword-to-str
  "Converts a keyword to a string without the colon."
  [keyword]
  (let [s (str keyword)]
    (subs s 1 (count s))))


(defn coll-types
  "Returns all types present in collection, excluding nil."
  [coll]
  (set (remove nil? (set (map type coll)))))


(defn rows-to-column-vecs
  "Takes a vector of row maps and returns a map of column vectors."
  [vec-of-row-maps]
  (let [col-names (set (flatten (map #(keys %)
                                     vec-of-row-maps)))]
    (into {}
          (zipmap col-names
                  (map (fn [k]
                         (vec (map #(k %)
                                   vec-of-row-maps)))
                       col-names)))))


(defn transpose-vectors
  [nested-vectors]
  (apply mapv vector nested-vectors))


(defn index-bag
  "Returns a map of the values of ks in the xrel mapped to a
  set of the maps in xrel with the corresponding values of ks. Like the `index`
  function in `clojure.set` except return \"bags\" (lists) instead of sets. In
  other words, it does not reduce the outputs to only the distinct values."
  [xrel ks]
    (reduce
     (fn [m x]
       (let [ik (select-keys x ks)]
         (assoc m ik (conj (get m ik []) x))))
     {} xrel))
