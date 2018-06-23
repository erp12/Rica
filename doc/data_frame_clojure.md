# DataFrame: A Clojure Data Structure

Rica implements a new type, called DataFrame, for storing data that is structure, relational, and/or tabular. A DataFrame is a data structure that is indexed with respect to rows and associative with respect to columns, much like a table in a typical SQL database.

The Rica DataFrame type implements `clojure.lang.Associative` and `clojure.lang.Indexed` so that instances of DataFrame can be manipulated using the built-in functions that Clojure uses to manipuate its other data structres.


## DataFrame Fields

The DataFrame type has two fields: a map of columns, and a schema.

A DataFrame's map of columns is what holds the data stored in the data strucutre. There is one key-value pair for each column of data, where the key denotes the column name and the value is a Column type vector


## DataFrame Columns

Each
