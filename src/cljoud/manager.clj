(ns cljoud.manager
  (:require [clojure.string :as str] [cljs.core.match :refer-macros [match]])
  (:use [co.paralleluniverse.pulsar core actors] [cljoud.tcp])
  (:refer-clojure :exclude [promise await]))

(defn freceive [from
                manager
                socket
                function]
  (let [raw-msg (str/split (srecv socket) #" " 2)
        msg-type (first raw-msg)
        msg (last raw-msg)]
    (function from manager msg-type msg)))

(defn node-matcher [from
                    manager
                    msg-type
                    msg]
  (match msg-type
         "register" (! manager [:register from msg])
         :else (! manager [:unknown from msg])))

(defsfn node [manager socket]
  (let [frecv (spawn-fiber freceive @self manager socket node-matcher)]
    (do
      (receive 
        [:id msg] (ssend socket (str/join " " ["id" msg])))
      (join frecv)
      (close socket))))

(defsfn client [manager socket])

(defsfn node-manager
  (let [nodes (transient [])
        last-node-id (atom 0)]
    (loop []
      (receive
        [:register from msg] (do
                               (conj! nodes @last-node-id)
                               (! from [:id (str @last-node-id)])
                               (swap! last-node-id #(+ %1 1))))
      (recur))))

(defsfn node-listener [manager socket]
  (loop []
    (let cs [(listen socket)]
      (spawn node manager cs))
    (recur)))

(defsfn client-listener [manager socket]
  (loop []
    (let cs [(listen socket)]
      (spawn client manager cs))
    (recur)))


(def -main [& args]
  (let [nm (spawn node-manager)
        nl (spawn node-listener nm (create-server-socket 8080))]
    (do
      (join nm)
      (join nl))))

