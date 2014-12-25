(ns cljoud.tcp)

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
    (let [writer (io/writer socket)]
      (.write writer msg)
      (.flush writer)))
  (srecv [x] (.readLine (io/reader socket)))
  (sclose [x] (.close socket)))

(defn create-server-socket [port] (println "Create server socket") (Soc. (ServerSocket. port)))
(defn create-client-socket [host port] (Soc. (Socket. host port)))
