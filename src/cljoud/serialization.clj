(ns cljoud.serialization
  (:use [cljoud common])
  (:require [clojure.data.json :as json])
  (:import [java.nio.charset Charset]))

(defn deserialize [data]
  (let [q (json/read-str data)]
    q))
(defn serialize [data]
  (json/write-str data))