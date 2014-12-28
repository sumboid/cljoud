(ns cljoud.core-test
  (:require [clojure.test :refer :all]
            [cljoud.node :refer :all]))

(deftest a-test
  (testing "Node test"
    (is (= [2 3 4] (do-map "f" "(defn f [x] (inc x))" [1 2 3])))))
