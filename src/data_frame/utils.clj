(ns data-frame.utils)


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
