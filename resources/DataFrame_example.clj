(require '[rica.core :refer [create-data-frame]])

(def users
  (create-data-frame :id [0 1 2]
                     :username ["alice" "bob" "eddie"]
                     :public-profile [false false true]))

(get users :username)
; #rica/Column ["alice" "bob" "eddie"]

(conj (get users :username) "robert")
; #rica/Column ["alice" "bob" "eddie" "robert"]

;(conj (get users :username) 5)
; Exception Cannot add class java.lang.Long to class java.lang.String column.

(assoc users :column-with-nils [\a nil \c])
; #rica/DataFrame <:id(class java.lang.Long) :username(class java.lang.String) :public-profile(class java.lang.Boolean) :column-with-nils(class java.lang.Character) >

(second users)
; #ordered/map ([:id 1] [:username "bob"] [:public-profile false])

(nth users 2)
; #ordered/map ([:id 2] [:username "eddie"] [:public-profile true])
