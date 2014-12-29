(ns cljoud.example.api)
(defn inc-m [x] (+ x 1))
(defn dec-m [x] (- x 1))
(defn sleep [x] (do (Thread/sleep (* x 1000)) x))
