(ns cljoud.example.client
  (:use cljoud.client))
(def conn (cloudrc "127.0.0.1:2000"))
(def conn2 (cloudrc "127.0.0.1:2000"))
(defn-remote conn cljoud.example.api/dec-m)
(defn-remote conn2 cljoud.example.api/inc-m)
(def m [1 2 3])
(defn -main [& args]
  (println m)
  (println (rmap dec-m m))
  (println (rmap inc-m m)))

