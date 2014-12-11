(defproject manager "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [co.paralleluniverse/pulsar "0.6.1"]]
  :java-agents [[co.paralleluniverse/quasar-core "0.6.1"]]
  :main "manager.core"
)
