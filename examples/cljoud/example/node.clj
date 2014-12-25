(ns cljoud.example.node
  (:use [cljoud node]))
(defn -main[& args]
  (dotimes [i 4]
        (.start (Thread. (start-node (+ 4000 i) "127.0.0.1:8000")))))
