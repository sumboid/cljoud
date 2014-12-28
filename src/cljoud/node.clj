(ns cljoud.node
  (:use [co.paralleluniverse.pulsar core actors] [cljoud tcp serialization common])
  (:refer-clojure :exclude [promise await]))

;(defn call [^String nm & args]
;    (when-let [fun (ns-resolve *ns* (symbol nm))]
;        (apply fun args)))

;(defn do-map[func-name func-code params]
;  (create-ns 'user)
;  ;;(binding [*ns* 'user]
;    (intern 'user (symbol func-name) (eval (read-string func-code)))
;    (let [result (map (fn [x] (call func-name x)) params)]
;      (remove-ns 'user)
;    result))

;(defn handle-request [msg]
;  (let [tid (first msg)
;        data (second (second msg))
;        func-name (first data)
;        func-code (nth data 1)
;        params (deserialize (nth data 2))]
;    (do-map func-name func-code params)))

;(defn start-node [port manager-addr]
;  (let [ [maddr mport] (host-port manager-addr)
;         node-soc (create-client-socket port)
;         wm (worker)
;         mc (spawn-fiber manager-client wm node-soc)
;         ml (spawn-fiber manager-listener wm node-soc)]
;    (do
;      (join ml))))

(defsfn worker [node]
  (loop []
    (receive
      [:subtask id sid subtask]
      (! node [:result @self id sid nil])) ; TODO: nil <- result
    (recur)))

(defsfn node [host port threads]
  (do
    (set-state! { :workers (map (fn [x] [(spawn worker @self) false]) (repeat threads 0))
                 :node-id 0 
                 :subtasks []})
    (let [socket (create-client-socket host port)]
      (ssend socket (serialize { :type "register"
                                :threads  threads
                                :host host
                                :port port })))
    (loop []
      (let [workers (get @state :workers)
            node-id (get @state :node-id)
            subtasks (get @state :subtasks)]
        (receive
          [:result f id sid result]
          (let [socket (create-client-socket host port)
                filtered-workers (filter #(not (= f (first %))) workers)
                current-worker (first (filter #(= f (first %)) workers))]
            (ssend socket (serialize {:node-id node-id
                                      :id id
                                      :subid sid
                                      :result result }))
            (set-state! { :workers (cons [(first current-worker) false] filtered-workers) 
                         :node-id node-id
                         :subtasks subtasks })
            (! @self [:try-execute]))

          [:subtask id sid subtask]
          (do
            (set-state! { :workers workers
                         :node-id node-id
                         :subtasks (cons { :id id :sid sid :subtask subtask } subtasks) })
            (! @self [:try-execute]))

          [:id nid] 
          (set-state! { :workers workers
                       :node-id nid
                       :subtasks subtasks })

          [:try-execute]
          (if-let [fworkers (filter #(= false (second %)) workers)]
            (doseq [w fworkers]
              (if-let [subtask (first subtasks)
                       id (get subtask :id)
                       sid (get subtask :sid)
                       st (get subtask :subtask)
                       filtered-workers (filter #(not (= w (first %))) workers)
                       current-worker (first (filter #(= w (first %)) workers))]
                (! (first w) [:subtask id sid st])
                (set-state! { :workers (cons [(first current-worker) true] filtered-workers)
                             :node-id node-id
                             :subtasks (filter #(not (= id (get % :id))) subtasks) }))))
          [:ok] nil)))
    (recur)))



(defsfn link [n cs] 
  (future
    (let [msg (srecv socket)
          pmsg (deserialize msg)]
      (do
        (println msg)
        (println pmsg)
        (println manager)
        (case (get pmsg :type)
          "id" (! n [:id (get pmsg :id)])
          "ok" nil
          "subtask"  (! manager [:subtask (get pmsg :id) (get pmsg :sid) (get pmsg :subtask)])
          (! manager [:unknown from pmsg]))))))

(defn listener [n socket]
  (future
    (loop []
      (let [cs (listen socket)]
        (spawn link n cs))
      (recur))))

(defn -main [& args]
  (let [n (spawn node "localhost" 8000 4)
        l-soc (create-server-socket 7777)
        l (spawn-fiber listener n l-soc)]
    (do
      (join n)
      (join l))))
