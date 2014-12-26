(ns cljoud.tcp
  (:require [clojure.string :as str]))

(require '[clojure.java.io :as io])
(import '(java.net ServerSocket) '(java.net Socket))

(defprotocol S
  (listen [x])
  (ssend [x msg])
  (srecv [x])
  (sclose[x]))

(deftype Soc [socket]
  S
  (listen [x] (Soc. (.accept socket)))
  (ssend [x msg]
    (println msg)
    (with-open [writer (io/writer socket)]   ;; >Should be used inside with-open to ensure the Writer is properly closed.
      (.write writer (str/join [(+ 1 (count msg)) "\n" msg]))
      (.flush writer)))
  (srecv [x] (let [in (io/input-stream socket)
                   size (read-string (.readLine (io/reader socket)))
                   buf (byte-array size)
                   rsize (.read in buf)] (apply str (map char buf))))
  (sclose [x] (.close socket)))


(defn create-server-socket [port] (println "Create server socket") (Soc. (ServerSocket. port)))
(defn create-client-socket [host port] (Soc. (Socket. host port)))
