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

(defn node-receive [from manager socket]
  (future
    (let [msg (srecv socket)
          pmsg (deserialize msg)]
      (do
        (println msg)
        (println pmsg)
        (println manager)
        (case (get pmsg :type)
          "register" (! manager [:register from pmsg])
          "subtask"  (! manager [:subtask from pmsg])
          "quit"  (! manager [:quit from pmsg])
          (! manager [:unknown from pmsg]))))))

(defn client-receive [from manager socket]
  (future
    (let [msg (srecv socket)
          pmsg (deserialize msg)]
      (do
        (println msg)
        (println pmsg)
        (println manager)
        (case (get pmsg :type)
          "task" (! manager [:new-task from (get pmsg :task)])
          "subscribe"  (! manager [:subscribe from (get pmsg :task-id)])
          "progress"  (! manager [:progress from (get pmsg :task-id)])
          (! manager [:unknown from pmsg]))))))


(defsfn node [manager socket]
  (let [umself @self
        frecv (spawn-fiber node-receive umself manager socket)]
    (do
      (receive 
        [:id id] (do
                    (ssend socket (serialize {:type "id" :id id})))
        [:subtask id] (do
                    (ssend socket (serialize {:type "id" :id id})))
        [:ok] (do
                (ssend socket (serialize {:type "ok"}))))
      (join frecv)
      (sclose socket))))

(defsfn client [manager socket]
  (let [umself @self
        frecv (spawn-fiber client-receive umself manager socket)]
    (do
      (receive 
        [:id id] (do
                   (ssend socket (serialize {:type "id" :id id})))
        [:result result] (do
                    (ssend socket (serialize result))) ; serializes result
        [:progress task-id progress] (do
                               (ssend socket (serialize {:task-id task-id :progress progress}))))
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
                                       port (get msg :port)]
                                   (! from [:id last-node-id])
                                   (set-state! { :nodes (cons nodes { :id last-node-id
                                                                     :info { :tds tds
                                                                            :host host
                                                                            :port port}})
                                                :last-node-id (+ last-node-id 1) })
                                   (! manager [:node last-node-id tds])))
          [:subtask from msg] (do
                                (let [node-id (get msg :node-id)
                                      id (get msg :id)
                                      subid (get msg :subid)
                                      result (get msg :result)]
                                  (! from [:ok])
                                  (! manager node-id id subid result)))

          [:send-subtask node-id id subid subtask]
          (let [node (first (filter #(= node-id (get % :id)) nodes))
                node-info (get node :info)
                host (get node-info :host)
                port (get node-info :port)
                socket (create-client-socket host port)]
            (do
              (ssend socket (serialize {:id id :subid subid :subtask subtask}))
              (sclose socket)))

          [:unknown from msg] (println "WAT")
          :else (println "Unknown message")))
      (recur))))

(defsfn manager []
  (do
    (set-state! { :nodes [] :tasks [] :last-task-id 0 :сomplete-subtasks [] :subtasks [] :node-manager nil})
    ; node    <- { id subtasks threads-number }
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
          (do
              (println "registered new node-manager: " nm-ref)
              (println nodes tasks last-task-id complete-subtasks subtasks subscribers nm-ref)
              (set-state! { :nodes nodes
                           :tasks tasks
                           :last-task-id last-task-id
                           :complete-subtasks complete-subtasks
                           :subtasks subtasks
                           :subscribers subscribers
                           :node-manager nm-ref }))

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

            (! from [:task-id last-task-id])) ; send task id to client

          [:subtask node-id id subid result]
          (let [filtered-tasks (filter #(not (= id (get % id))) tasks)
                current-task (first (filter #(= id (get % id)) tasks))
                ct-id (get :id current-task)
                ct-st-number (get :st-number current-task)
                ct-cst-number (+ 1 (get :cst-number current-task))

                filtered-nodes (filter #(not (= node-id (get % :id) nodes)))
                current-node (first (filter #(= node-id (get % :id)) nodes))
                unode-id (get current-node :id)
                unode-tds (get current-node :tds)
                unode-sts (filter #(not (and (= id (get % :id)) (= subid (get % :subid)))) (get current-node :subtasks))]
            (do
              (set-state! { :nodes (cons {:id unode-id :tds unode-tds :subtasks unode-sts } filtered-nodes)
                           :tasks (cons filtered-tasks { :id ct-id :st-number ct-st-number :cst-number ct-cst-number })
                           :last-task-id last-task-id
                           :complete-subtasks (cons complete-subtasks { :id id :sid subid :result result })
                           :subtasks (filter #(not (and (= id (get % :id)) (= subid (get % :subid)))) subtasks)
                           :subscribers subscribers
                           :node-manager node-manager })
              (! @self [:check-complete])))

          [:progress from task-id]
          (let [task (first (filter #(= task-id (get % :id) tasks)))
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
                (let [css (filter #(= task-id (get % :id)) complete-subtasks)
                      scss (sort #(compare (get %1 :sid) (get %2 :sid)) css)
                      results (map #(get % :result) scss)]
                  ; merge results of subtasks
                  ; send result to subsriber
                  (set-state! { :nodes nodes
                               :tasks tasks
                               :last-task-id last-task-id
                               :complete-subtasks (filter #(not (= task-id (get % :id))) complete-subtasks)
                               :subtasks subtasks
                               :subscribers (filter #(not (and
                                                            (= subscriber (get % :subscriber))
                                                            (= task-id (get % :task-id)))) subscribers)
                               :node-manager node-manager }))
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
(defn node-listener [manager socket]
  (future
    (loop []
      (let [cs (listen socket)]
        (spawn node manager cs))
      (recur))))

(defn client-listener [manager socket]
  (future
    (loop []
      (let [cs (listen socket)]
        (spawn client manager cs))
      (recur))))


(defn -main [& args]
  (let [m (spawn manager)
        nm (spawn node-manager m)
        nl-soc (create-server-socket 8000)
        nl (spawn-fiber node-listener nm nl-soc)
        client-soc (create-server-socket 8080)
        cl (spawn-fiber client-listener m client-soc)]
    (do
      (join m)
      (join nm)
      (join nl)
      (join cl))))

