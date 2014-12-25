(ns cljoud.common
  (:use [clojure.string :only [split]]))
(defn host-port
 "get host and port from connection string"
 [connection-string]
   (let [[host port] (split connection-string #":")]
      [host (Integer/valueOf ^String port)]))

(def ^:dynamic *debug* true)