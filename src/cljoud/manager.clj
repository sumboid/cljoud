(ns cljoud.manager
  (:require [clojure.string :as str] [cljs.core.match :refer-macros [match]])
  (:use [co.paralleluniverse.pulsar core actors] [cljoud tcp serialization])
  (:refer-clojure :exclude [promise await]))

(def next-task-id (atom 0))

(defsfn gen-next-task-id []
  swap! next-task-id inc)

(defsfn make-request [tid func-name func-code params]
  (let [serialized-params (serialize params)]
    (serialize [:request [tid func-name func-code serialized-params]])))

(defsfn freceive [from
                  manager
                  socket
                  function]
  (println from)
  (future
    (let [raw-msg (deserialize (srecv socket))
          msg-type (first raw-msg)
          msg (last raw-msg)]
      (println raw-msg)
      (function from manager msg-type msg))))

(defsfn node-matcher [from
                      manager
                      msg-type
                      msg]
  (println from msg)
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

(defsfn client-req-gen [from
                        manager
                        msg-type
                        data]
  (println "DATA" (deserialize data)))

  (let [tid (first data)
        func-name (nth data 1)
        func-code (nth data 2)
        coll (deserialize (nth data 3))
        len (count coll)
        nodes 4;; <- avail nodes TODO
        step (quot len nodes)
        manager-tid (gen-next-task-id)]
    (loop [offset 0 queries '() tail coll]
      (if (> (+ step offset) len)
        (cons (make-request [offset manager-tid] func-name func-code tail) queries)
        (recur (+ offset step) (cons (make-request [offset manager-tid] func-name func-code (take step tail)) queries) (drop step tail))))))

(defsfn client-req-handler [from
                            manager
                            msg-type
                            data]
  (let [queries (client-req-gen from manager msg-type data)]
    ;; send queries to nodes here
    (println queries)))

(defsfn client [socket]
  (let [umself @self
        frecv (spawn-fiber freceive umself nil socket client-req-handler)]
    (do
      (receive
        [:id msg] (do
                    (ssend socket (str (str/join " " ["id" msg])))))
      (join frecv)
      (sclose socket))))

(defn node-listener [manager socket]
  (future
    (loop []
      (let [cs (listen socket)]
        (spawn node manager cs))
      (recur))))

(defn client-listener [socket]
   (future
     (loop []
      (let [cs (listen socket)]
        (spawn client cs))
      (recur))))


(defn -main [& args]
  (let [nm (spawn node-manager)
        nl-soc (create-server-socket 8000)
        nl (spawn-fiber node-listener nm nl-soc)
        client-soc (create-server-socket 8080)
        cl (spawn-fiber client-listener client-soc)]
    (do
      (join nm)
      (join nl)
      (join cl))))

