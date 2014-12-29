(ns cljoud.example.client
  (:use cljoud.client))
(try
  (def client (cloudrc "127.0.0.1:8080"))
  (defn-remote client cljoud.example.api/inc-m :async? false)
  (defn-remote client cljoud.example.api/dec-m :async? true)
(catch Exception e (str "caught exception: " (.getMessage e))))

(def m [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16])
(defn -main [& args]
  (println "Applying map() to " m)

  (let [m-inc (rmap inc-m m)]
    (println "After inc-m " m-inc)
    ;(let [c (rmap dec-m m)]
    ;  (println "Progress " (get-progress c)) ;(* 100 (get-progress c)) "%")
    ;  (println "After dec-m "(join c)))))
    ))
