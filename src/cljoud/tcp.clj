(ns cljoud.tcp)

(require '[clojure.java.io :as io])
(import '(java.net ServerSocket) '(java.net Socket))

(defprotocol S
  (listen [x])
  (ssend [x msg])
  (srecv [x])
  (close[x]))

(deftype Soc [socket]
  S
  (listen [x] (.accept socket))
  (ssend [x msg]
    (let [writer (io/writer socket)]
      (.write writer msg)
      (.flush writer)))
  (srecv [x] (.readLine (io/reader socket)))
  (close [x] (.close socket)))

(defn create-server-socket [port] (Soc. (ServerSocket. port)))
(defn create-client-socket [host port] (Soc. (Socket. host port)))
