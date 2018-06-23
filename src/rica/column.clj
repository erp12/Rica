(ns rica.column
  "The `column` namespace contains an implementation of a Column data strucutre
  and functions to help create Columns out of other collections.
  A Column is simply a typed PersistentVector that allows nils."
  (:require [rica.utils :as u])
  (:import (clojure.lang IPersistentVector)))


(defn column-type-mismatch
  [actual-dtype expected-dtype]
  (throw (Exception.
           (str "Cannot add " actual-dtype " to " expected-dtype " column."))))


(deftype Column
  [data dtype]

  IPersistentVector
  (assocN [self i val]
    (if (or (nil? val)
            (instance? dtype val))
      (Column. (.assocN data i val) dtype)
      (column-type-mismatch (type val) dtype)))

  (cons [self o]
    (if (or (nil? o)
            (instance? dtype o))
      (Column. (.cons data o) dtype)
      (column-type-mismatch (type o) dtype)))

  ;; Associative
  (containsKey [self key]
    (.containsKey data key))

  (entryAt [self key]
    (.entryAt data key))

  (assoc [self key val]
    (if (or (nil? val)
            (instance? dtype val))
      (Column. (.assocN data key val) dtype)
      (column-type-mismatch (type val) dtype)))

  ;; IPersistentStack
  (peek [self]
    (.peek data))

  (pop [self]
    (Column. (.pop data) dtype))

  ;; Reversible
  (rseq [self]
    (.rseq data))

  ;; Indexed
  (nth [self i]
    (.nth data i))

  (nth [self i notFound]
    (.nth data i notFound))

  ;; Counted
  (count [self]
    (.count data))

  ;; ILookup
  (valAt [self key]
    (.valAt data key))

  (valAt [self key notFound]
    (.valAt data key notFound))

  ;; IPersistentCollection
  (empty [self]
    (Column. (.empty data) dtype))

  (equiv [self o]
    (and (isa? (class o) Column)
         (= dtype (.dtype o))
         (= (seq data) (seq o))))

  ;; Seqable
  (seq [self]
    (.seq data))

  Object
  (toString [self]
    (str "Column<" dtype "> " data))
  )


(defmethod print-method Column [o ^java.io.Writer w]
  (.write w "#rica/Column ")
  (print-method (.data o) w))


(defn create-column
  "Creates a new Column containing the contents of coll. The type of the
  Column's contents can either be given (dytpe) or infered. If a column type
  is infered from a collection with all nils, java.lang.Boolean is used.

  A Column is a indexed structure whose elements are all either nil or the same
  type."
  ([coll dtype]
  (if (u/coll-of-type? coll dtype)
    (->Column (vec coll) dtype)
    (throw (Exception. "Columns must be of uniform type."))))
  ([coll]
    (let [col-types (u/coll-types coll)]
      (cond
        (empty? col-types)
        (->Column (vec coll) java.lang.Boolean)

        (= (count col-types) 1)
        (->Column (vec coll) (first col-types))

        :else
        (column-type-mismatch (first col-types) (second col-types))))))


(defn column-type
  "Returns the type of the given column. All elements in the column are either
  this type or nil."
  [col]
  (.dtype col))
