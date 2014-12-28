(ns cljoud.node
  (:use [co.paralleluniverse.pulsar core actors] [cljoud tcp serialization common])
  (:refer-clojure :exclude [promise await]))

(defn call [^String nm & args]
    (when-let [fun (ns-resolve *ns* (symbol nm))]
        (apply fun args)))

(defn do-map[func-name func-code params]
  (create-ns 'user)
  ;;(binding [*ns* 'user]
    ;(intern 'user (symbol func-name)
    (eval (read-string func-code))
    (let [result (map (fn [x] (call func-name x)) params)]
      (remove-ns 'user)
      (println result)
    result))

(defn handle-request [msg]
 (let [func-name (first msg)
        func-code (nth msg 1)
        params (nth msg 2)]
    (do-map func-name func-code params)))

(defsfn worker [node]
  (loop []
    (receive
      [:subtask id sid subtask]
      (let [result (handle-request subtask)]
        (! node [:result @self id sid result])))
      (recur)))

(defsfn node [host port threads listening_port listening_host]
  (do
    (set-state! { :workers (map (fn [x] [(spawn worker @self) false]) (repeat threads 0))
                 :node-id 0 
                 :subtasks []})
    (let [socket (create-client-socket host port)]
      (ssend socket (serialize { :type "register"
                                :threads  threads
                                :host listening_host
                                :port listening_port })))
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
            ;;(println "Subtask recieved id " id ", sid " sid ", content " subtask)
            (set-state! { :workers workers
                         :node-id node-id
                         :subtasks (cons { :id id :sid sid :subtask subtask } subtasks) })
            (! @self [:try-execute]))

          [:id nid]
          (do
            (println "id received " nid)
          (set-state! { :workers workers
                       :node-id nid
                       :subtasks subtasks }))
                                                                                           ну
          [:try-execute]
          (do
          (let [fworkers (filter #(= false (second %)) workers)]
            (doseq [w fworkers]
              (if-let [subtask (first (get @state :subtasks))]
                (let [id (get subtask :id)
                       sid (get subtask :sid)
                       st (get subtask :subtask)
                       filtered-workers (filter #(not (= (first w) (first %))) workers)
                       current-worker (first (filter #(= (first w) (first %)) workers))]
                (! (first w) [:subtask id sid st])
                (set-state! { :workers (cons  [(first current-worker) true] filtered-workers)
                              :node-id node-id
                              :subtasks (filter #(not (= id (get % :id))) subtasks) }))))))
          [:ok] nil)
    (recur)))))



(defsfn link [manager socket]
  (future
    (let [msg (srecv socket)
          pmsg (deserialize msg)]
      (do
        ;(println msg)
        ;;(println pmsg)
        ;(println manager)
        (case (get pmsg :type)
          "id" (! manager [:id (get pmsg :id)])
          "ok" nil
          "subtask"  (! manager [:subtask (get pmsg :id) (get pmsg :subid) (get pmsg :subtask)])
          (! manager [:unknown nil pmsg]))))))

(defn listener [n socket]
  (future
    (loop []
      (let [cs (listen socket)]
        (spawn link n cs))
      (recur))))

(defn -main [& args]
  (let [ listening_port 7777
         listening_host "localhost"
         n (spawn node "localhost" 8000 4 listening_port listening_host)
        l-soc (create-server-socket listening_port)
        l (spawn-fiber listener n l-soc)]
    (do
      (join n)
      (join l))))
