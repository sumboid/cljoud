(defproject cljoud "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://sumboid.github.io/cljoud/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :main ^:skip-aot cljoud.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
