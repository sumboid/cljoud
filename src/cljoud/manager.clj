(ns cljoud.manager
  (:require [clojure.string :as str] [cljs.core.match :refer-macros [match]])
  (:use [co.paralleluniverse.pulsar core actors] [cljoud.tcp])
  (:refer-clojure :exclude [promise await]))

(defsfn freceive [from
                  manager
                  socket
                  function]
  (println from)
  (future
    (let [raw-msg (str/split (srecv socket) #" " 2)
          msg-type (first raw-msg)
          msg (last raw-msg)]
      (function from manager msg-type msg))))

(defsfn node-matcher [from
                      manager
                      msg-type
                      msg]
  (println from)
  (case msg-type
    "register" (! manager [:register from msg])
    (! manager [:unknown from msg])))

(defsfn node [manager socket]
  (let [umself @self
        frecv (spawn-fiber freceive umself manager socket node-matcher)]
    (do
      (receive 
        [:id msg] (do
                    (ssend socket (str (str/join " " ["id" msg])))))
      (join frecv)
      (sclose socket))))


(defsfn node-manager []
  (set-state! { :nodes [] :last-node-id 0 })
    (loop []
      (let [nodes (get @state :nodes) last-node-id (get @state :last-node-id)]
      (receive
        [:register from msg] (do
                               (! from [:id last-node-id])
                               (set-state! { :nodes (conj nodes last-node-id) :last-node-id (+ last-node-id 1) }))))
      (recur)))

(defn node-listener [manager socket]
  (future
    (loop []
      (let [cs (listen socket)]
        (spawn node manager cs))
      (recur))))

;(defsfn client-listener [manager socket]
;  (loop []
;    (let [cs (listen socket)]
;      (spawn client manager cs))
;    (recur)))


(defn -main [& args]
  (let [nm (spawn node-manager)
        nl-soc (create-server-socket 8080)
        nl (spawn-fiber node-listener nm nl-soc)]
    (do
      (join nm)
      (join nl))))

