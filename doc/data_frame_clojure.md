# DataFrame Data Structure

Rica implements a new type, called DataFrame, for storing data that is structure, relational, and/or tabular. A DataFrame is a data structure that is indexed with respect to rows and associative with respect to columns, much like a table in a typical SQL database.

The Rica DataFrame type implements `clojure.lang.Associative` and `clojure.lang.Indexed` so that instances of DataFrame can be manipulated using the built-in functions that Clojure uses to manipuate its other data structres.


## DataFrame Fields

The DataFrame type has two fields: a map of columns, and a schema.

A DataFrame's map of columns is what holds the data stored in the data strucutre. There is one key-value pair for each column of data, where the key denotes the column name and the value is a Column type. A Column is Rica's implementaiton of a typed vector, and is described more in the next section.

A DataFrame's schema is used to denote 1) the order of columns and 2) the data type (class) of each column. The mechanism used to implement DataFrame schemas is an ordered-map.


## DataFrame Columns

When operating on the columns of a DataFrame things feel a lot like a Map.

```clojure
(require '[rica.core :refer [create-data-frame]])

(def users
  (create-data-frame :id [0 1 2]
                     :username ["alice" "bob" "eddie"]
                     :public-profile [false false true]))

(get users :username)
; #rica/Column ["alice" "bob" "eddie"]

(conj (get users :username) 5)
; Exception Cannot add class java.lang.Long to class java.lang.String column.

(conj (get users :username) "robert")
; #rica/Column ["alice" "bob" "eddie" "robert"]

(assoc users :column-with-nils [\a nil \c])
; #rica/DataFrame <:id(class java.lang.Long) :username(class java.lang.String) :public-profile(class java.lang.Boolean) :column-with-nils(class java.lang.Character) >

(contains? users :foo)
; false

(contains? users :id)
; true

```


## DataFrame Rows

Although a DataFrame does not store data on a per row basis, it is often times needed to treat a DataFrame as an indexed data structure of rows.

The typical clojure functions which operate on indexed data structures will (ie `nth`, `first`, etc) will return a single row in the form of an ordered-map.

```clojure
(second users)
; #ordered/map ([:id 1] [:username "bob"] [:public-profile false])

(nth users 2)
; #ordered/map ([:id 2] [:username "eddie"] [:public-profile true])
```

An entire DataFrame can be converted into a sequence of rows with `seq`.

```clojure
(seq users)
; (#ordered/map ([:id 0] [:username "alice"] [:public-profile false]) #ordered/map ([:id 1] [:username "bob"] [:public-profile false]) #ordered/map ([:id 2] [:username "eddie"] [:public-profile true]))

(filter #(not (:public-profile %)) (seq users))
; (#ordered/map ([:id 0] [:username "alice"] [:public-profile false]) #ordered/map ([:id 1] [:username "bob"] [:public-profile false]))
```

The data-frame API in `rica.core` provides functions to turn sequences of rows back into DataFrame.
