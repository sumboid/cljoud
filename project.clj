(defproject cljoud "0.1.0-SNAPSHOT"
  :description "Realization of map() in cloud"
  :url "http://sumboid.github.io/cljoud/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                  [org.clojure/clojure "1.6.0"]
                  [cheshire "5.3.1"]
                  [co.paralleluniverse/pulsar "0.6.2"]
                  [org.clojure/core.match "0.3.0-alpha4"]
                ]
  :java-agents [[co.paralleluniverse/quasar-core "0.6.2"]]

  :profiles {:example {:source-paths ["examples"]}
             :clojure16 {:dependencies [[org.clojure/clojure "1.6.0"]]}}
  :aliases {"run-example-manager" ["with-profile" "default,clojure16,example" "run" "-m" "cljoud.example.manager"]
             "run-example-node" ["with-profile" "default,clojure16,example" "run" "-m" "cljoud.example.node"]
             "run-example-client" ["with-profile" "default,clojure16,example" "run" "-m" "cljoud.example.client"]
             "run-manager" ["with-profile" "default,clojure16" "run" "-m" "cljoud.manager"]
             "test-all" ["with-profile" "default,clojure15:default,clojure16" "test"]})
