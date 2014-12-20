(ns cljoud.example.manager
  (:use [cljoud manager]))
(defn -main [& args]
  (start-manager 2000)
  (println "Example manager started on port 2000"))
