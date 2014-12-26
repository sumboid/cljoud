(ns cljoud.manager
  (:require [clojure.string :as str] [cheshire.core :refer :all])
  (:use [co.paralleluniverse.pulsar core actors] [cljoud tcp serialization])
  (:refer-clojure :exclude [promise await]))

(def next-task-id (atom 0))

(def nm-ref (atom nil))

(defsfn gen-next-task-id []
  swap! next-task-id inc)

(defsfn make-request [tid func-name func-code params]
  (let [serialized-params (serialize params)]
    (serialize [:request [tid func-name func-code serialized-params]])))

(defn freceive [from socket]
  (future
    (let [msg (srecv socket)
          pmsg (parse-string msg true)]
      (do
        (println msg)
        (println (parse-string msg))
        (println nm-ref)
        (case (get pmsg :type)
          "register" (! nm-ref [:register from pmsg])
          (! nm-ref [:unknown from pmsg]))))))

(defsfn node [socket]
  (let [umself @self
        frecv (spawn-fiber freceive umself socket)]
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
      (println "Prepare your anus")
      (receive
        [:register from msg] (do
                               (println "register message")
                               (! from [:id last-node-id])
                               (set-state! { :nodes (conj nodes last-node-id) :last-node-id (+ last-node-id 1) }))
        [:unknown from msg] (println "WAT")))
    (recur)))

(defsfn client-req-gen [from
                        manager
                        msg-type
                        data]
  (println "DATA" (deserialize data))

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
        frecv (spawn-fiber freceive umself  socket)]
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
        (spawn node cs))
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
      (swap! nm-ref (fn [x] nm))
      (join nm)
      (join nl)
      (join cl))))

