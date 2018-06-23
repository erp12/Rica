(ns data-frame.utils
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


(defn rows-to-columns
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
