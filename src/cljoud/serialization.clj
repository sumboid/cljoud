(ns cljoud.serialization
  (:use [cljoud common])
  (:require [cheshire.core :as json])
  (:import [java.nio.charset Charset]))

(defn deserialize
  ([data] (deserialize data :buffer))
  ([data it]
  (let [jsonstr
        (case it
          :buffer (.toString (.decode (Charset/forName "UTF-8") data))
          :bytes (String. ^bytes ^String data "UTF-8")
          :string data)]
        (if *debug* (println (str "dbg:: " jsonstr)))
            (json/parse-string jsonstr true))))
(defn serialize
  ([data] (serialize data :buffer))
  ([data ot]
    (let [jsonstr (json/generate-string data)]
      (if *debug* (println (str "dbg:: " jsonstr)))
        (case ot
          :buffer (.encode (Charset/forName "UTF-8") jsonstr)
          :string jsonstr
          :bytes (.getBytes jsonstr "UTF-8")))))