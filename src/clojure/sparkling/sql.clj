(ns clojure.sparkling.sql)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic Spark management

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local")
              (conf/app-name "Clojure Spark example"))]
    (spark/spark-context c)))



(defonce sc (make-spark-context))


(comment

  (spark/take 4 area-rdd)
  (spark/take 113 min-price)
  (spark/take 113 max-price)
  (spark/take 4 area-rdd)
  (nth (spark/take 15 words-rdd) 13)
  (spark/get )

  (max (Integer/parseInt (nth x 1)) (Integer/parseInt (nth y 1)))

(def input-rdd (spark/text-file sc "../input/pp-monthly-update-new-version.csv"))
(def area-rdd (spark/map-to-pair index-on-area input-rdd))
(def max-price (spark/reduce-by-key (fn [x y] (max x y) ) area-rdd))
(def min-price (spark/reduce-by-key (fn [x y] (min x y) ) area-rdd))

)

(defn index-sell-price-by-area [l]
  (let [sx (s/split l #"\",\"")]
    (spark/tuple (nth sx 13) (Integer/parseInt (nth sx 1)))))


(defn get-sell-price-by-area [input-rdd] (spark/map-to-pair index-sell-price-by-area input-rdd))
(def max-price (fn [x y] (max x y) ))
(def min-price (fn [x y] (min x y) ))






(defn -main [& args]
  (let [input-rdd (spark/text-file sc "../input/pp-monthly-update-new-version.csv")
        area-rdd (spark/map-to-pair index-sell-price-by-area input-rdd)
        max-price-rdd (spark/reduce-by-key max-price area-rdd)
        min-price-rdd (spark/reduce-by-key min-price area-rdd)
        max-price (spark/take 10 max-price-rdd)
        min-price (spark/take 10 min-price-rdd)
        ]
        (println "max price:" max-price)
        (println "min price:" min-price)
        ))
Status API Training Shop Blog About
