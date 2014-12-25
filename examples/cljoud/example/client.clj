(ns cljoud.example.client
  (:use cljoud.client))
(try
  (def conn (cloudrc "127.0.0.1:8080"))
;;  (def conn2 (cloudrc "127.0.0.1:8080"))
  (defn-remote conn cljoud.example.api/dec-m)
;;  (defn-remote conn2 "cljoud.example.api/inc-m")
  (catch Exception e (str "caught exception: " (.getMessage e))))

(def m [1 2 3])
(defn -main [& args]
  (println m)
  (println (rmap dec-m m)))
 ;; (println (rmap inc-m m)))

