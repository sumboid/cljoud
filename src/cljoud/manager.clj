(ns cljoud.manager
  (:require [clojure.string :as str] [clojure.data.json :as json])
  (:use [co.paralleluniverse.pulsar core actors] [cljoud tcp serialization common])
  (:refer-clojure :exclude [promise await]))

(def next-task-id (atom 0))

(defsfn gen-next-task-id []
  swap! next-task-id inc)

(defsfn make-request [tid func-name func-code params]
  (let [serialized-params params]
    (serialize [:request [tid func-name func-code serialized-params]])))

(defn freceive [from manager socket]
  (future
    (let [msg (srecv socket)
          pmsg (json/read-str msg :key-fn keyword)]
      (do
        (println msg)
        (println pmsg)
        (println manager)
        (case (get pmsg :type)
          "register" (! manager [:register from pmsg])
          "task" 
          (! manager [:unknown from pmsg]))))))

(defsfn node [manager socket]
  (let [umself @self
        frecv (spawn-fiber freceive umself manager socket)]
    (do
      (receive 
        [:id msg] (do
                    (ssend socket (str (str/join " " ["id" msg])))))
      (join frecv)
      (sclose socket))))


(defsfn node-manager [manager]
  (do
    (set-state! { :nodes [] :last-node-id 0 })
    (! manager [:register @self])
    (loop []
      (let [nodes (get @state :nodes) last-node-id (get @state :last-node-id)]
        (receive
          [:register from msg] (do
                                 (let [tds (get msg :threads)
                                       host (get msg :host)
                                       port (read-string (get msg :port))]
                                   (! from [:id last-node-id])
                                   (set-state! { :nodes (cons nodes { :id last-node-id
                                                                     :info { :tds tds
                                                                            :host host
                                                                            :port port}})
                                                :last-node-id (+ last-node-id 1) })))
          [:unknown from msg] (println "WAT")
          :else (println "Unknown message")))
      (recur))))

(defsfn manager []
  (do
    (set-state! { :nodes [] :tasks [] :last-task-id 0 :сomplete-subtasks [] :subtasks [] :node-manager nil})
    ; node    <- { id subtasks-number complete-subtasks-number threads-number }
    ; task    <- { id task-data }
    ; subtask <- { node-id id subid subtask-data }
    ; complete-subtask <- { id subid result }

    (loop []
      (let [nodes (get @state :nodes)
            last-task-id (get @state :last-task-id)
            tasks (get @state :tasks)
            complete-subtasks (get @state :complete-subtasks)
            subtasks (get @state :subtasks)
            node-manager (get @state :node-manager)
            subscribers (get @state :subscribers)]

        (receive
          [:node id tds]
          (set-state! { :nodes (cons nodes {:id id :tds tds :subtasks [] })
                       :tasks tasks
                       :last-task-id last-task-id
                       :complete-subtasks complete-subtasks
                       :subtasks subtasks
                       :subscribers subscribers
                       :node-manager node-manager })

          [:register nm-ref]
          (set-state! { :nodes nodes
                       :tasks tasks
                       :last-task-id last-task-id
                       :complete-subtasks complete-subtasks
                       :subtasks subtasks
                       :subscribers subscribers
                       :node-manager nm-ref })

          [:new-task from task]
          (do
            ;; split task
            ;; task <- [last-task-id parts-number complete-parts-number]
            ;; send subtasks to nodes
            (set-state! { :nodes nodes
                         :tasks tasks
                         :last-task-id (+ 1 last-task-id)
                         :complete-subtasks complete-subtasks
                         :subtasks subtasks
                         :subscribers subscribers
                         :node-manager node-manager })

            (! from :task-id (- last-task-id 1))) ; send task id to client

          [:subtask id subid result]
          (let [filtered-tasks (filter #(not (= id (get % id))) tasks)
                current-task (first (filter #(= id (get % id)) tasks))
                ct-id (get :id current-task)
                ct-st-number (get :st-number current-task)
                ct-cst-number (+ 1 (get :cst-number current-task))]
            (do
              (set-state! { :nodes nodes
                           :tasks (cons filtered-tasks { :id ct-id :st-number ct-st-number :cst-number ct-cst-number })
                           :last-task-id last-task-id
                           :complete-subtasks (cons complete-subtasks { :id id :sid subid :result result })
                           :subtasks (filter #(and (= id (get % :id)) (= subid (get % :subid))) subtasks)
                           :subscribers subscribers
                           :node-manager node-manager })
              (! @self [:check-complete])))

            [:progress from task-id]
            (let [task (first (filter #(= id (get % id) tasks)))
                  st-number (get task :st-number)
                  cst-number (get task :cst-number)]
              (! from [:progress (/ cst-number (double st-number))]))

            [:subscribe from task-id]
            (do
              (set-state! { :nodes nodes
                           :tasks tasks
                           :last-task-id last-task-id
                           :complete-subtasks complete-subtasks
                           :subtasks subtasks
                           :subscribers (cons { :task-id task-id :subscriber from } subscribers)
                           :node-manager node-manager })
              (! @self [:check-complete]))

            [:check-complete]
            (doseq [s subscribers]
              (let [subscriber (get s :subscriber)
                    task-id (get s :task-id)
                    task (first (filter #(= task-id (get % :id)) tasks))
                    st-n (get task :st-number)
                    cst-n (get task :cst-number)]
                (if (= st-n cst-n) ; complete task was finded
                  (let [css (filter #(= task-id (get % :id)) complete-subtask)
                        scss (sort #(compare (get %1 :sid) (get %2 :sid)) css)
                        results (map #(get % :result) scss)]
                    ; merge results of subtasks
                    ; send result to subsriber
                    nil)
                  nil)))))
        (recur))))





(defsfn client-req-gen [from
                        manager
                        msg-type
                        data]
  (let [tid (first data)
        func-name (nth data 1)
        func-code (nth data 2)
        coll (first (nth data 3)) ;; J
        len (count coll)
        nodes 2;; <- avail nodes TODO
        step (quot len nodes)
        manager-tid (gen-next-task-id)]
    (if *debug*
      (println "TID: " tid ", FNAME " func-name ", FCODE " func-code ", COLL: " coll ", STEP " step ", LEN " len))
    (loop [offset 0 queries '() tail coll]
      (println offset)
      (if (or (>= (+ step offset) len) (= offset (* (dec nodes) step)))
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

