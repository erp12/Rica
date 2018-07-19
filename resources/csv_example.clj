(require '[rica.core :refer :all])
(require '[rica.io :refer :all])


(def apps
  (-> (from-csv "resources/apps_data.csv" true)
      (with-column :major_version #(Long/parseLong (:major_version %)))
      (with-column :minor_version #(Long/parseLong (:minor_version %)))))


(def pre-releases
  (-> apps
      (where #(zero? (:major_version %)))
      (order-by :minor_version)
      (with-column :version
                   #(str (:major_version %) "." (:minor_version %)))
      (select :app :version)))


(show pre-releases)

; |      :app | :version |
; |-----------+----------|
; |    Zoolab |      0.2 |
; | Lotstring |      0.5 |
; |  Wrapsafe |      0.6 |


(to-csv pre-releases "resources/prerelease_apps_data.csv" true :separator \|)

; Contents of "resources/prerelease_apps_data.csv"
; app|version
; Zoolab|0.2
; Lotstring|0.5
; Wrapsafe|0.6
