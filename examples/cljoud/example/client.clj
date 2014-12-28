(ns cljoud.example.client
  (:use cljoud.client))
(try
  (def client (cloudrc "127.0.0.1:8080"))
  (defn-remote client cljoud.example.api/dec-m :async? true)
(catch Exception e (str "caught exception: " (.getMessage e))))

(def m [1 2 3 4 5 6])
(defn -main [& args]
  (println "Applying map() to " m)
  (let [c (rmap dec-m m)]
    (println (get-progress c))
    (println (join client))))
