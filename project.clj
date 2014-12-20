(defproject cljoud "0.1.0-SNAPSHOT"
  :description "Realization of map() in cloud"
  :url "http://sumboid.github.io/cljoud/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :profiles {:example {:source-paths ["examples"]}}
  :aliases {"run-example-manager" ["with-profile" "default,clojure16,example" "run" "-m" "cljoud.example.manager"]
             "run-example-node" ["with-profile" "default,clojure16,example" "run" "-m" "cljoud.example.node"]
             "run-example-client" ["with-profile" "default,clojure16,example" "run" "-m" "cljoud.example.client"]
             "test-all" ["with-profile" "default,clojure15:default,clojure16" "test"]}
