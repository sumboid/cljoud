(ns cljoud.tcp
  (:require [clojure.string :as str]))

(require '[clojure.java.io :as io])
(import '(java.net ServerSocket) '(java.net Socket))

(defprotocol S
  (listen [x])
  (ssend [x msg])
  (srecv [x])
  (sclose[x]))
(defn read-n-bytes [reader num-bytes]
  (let [carr   (char-array num-bytes)
        n-read (.read reader carr 0 num-bytes)
        trimmed-carr (if (= n-read num-bytes) carr
                       (char-array n-read carr))]
    (line-seq (java.io.BufferedReader.
                (java.io.StringReader.
                  (apply str (seq trimmed-carr)))))))
(deftype Soc [socket]
  S
  (listen [x] (Soc. (.accept socket)))
  (ssend [x msg]
    (let [writer (io/writer socket)]
      (.write writer (str/join [(count msg) "\n" msg]))
      (.flush writer)))
  (srecv [x] (let [;;in (io/input-stream socket)
                   rdr (io/reader socket)
                   size (read-string (.readLine rdr))
                   bytes (first (read-n-bytes rdr size))] ;; first cause it returns (str)
               bytes))

  (sclose [x] (.close socket)))


(defn create-server-socket [port] (println "Create server socket") (Soc. (ServerSocket. port)))
(defn create-client-socket [host port] (Soc. (Socket. host port)))
