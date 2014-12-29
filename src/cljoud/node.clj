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
    ;(println result)
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
          (! node [:res @self id sid result])
          (! node [:try-execute])))
      (recur)))

(defsfn node [host port threads listening_port listening_host]
  (do
    (let [umself @self]
      (set-state! {:workers (map (fn [x] [(spawn worker umself) false]) (repeat threads 0))
                   :node-id 0
                   :subtasks []}))
    (let [socket (create-client-socket host port)]
      (ssend socket (serialize { :type "register"
                                :threads  threads
                                :host listening_host
                                :port listening_port })))
    (loop []
      (let [workers  (get @state :workers)
            node-id  (get @state :node-id)
            subtasks (get @state :subtasks)]
        (receive
          [:res f id sid result]
          (let [socket (create-client-socket host port)
                filtered-workers (filter #(not (= f (first %))) workers)
                current-worker (first (filter #(= f (first %)) workers))]
            (println "send subtask to manager:" id sid)
            (ssend socket (serialize {:type "subtask"
                                      :node-id node-id
                                      :id id
                                      :subid sid
                                      :result result }))
            (set-state! { :workers  (cons [(first current-worker) false] filtered-workers) 
                         :node-id  node-id
                         :subtasks subtasks })
            (! @self [:try-execute]))

          [:subtask id sid subtask]
          (do
            (println "Subtask recieved id " id ", sid " sid ", content " subtask)
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
          [:try-execute]
          (do
            (let [fws (map #(first %) (filter #(= false (second %)) workers))
                  sts (get @state :subtasks)
                  dist (map vector fws sts)

                  cfws (map #(first %) dist)
                  csts (map #(last %) dist)

                  fcfws (filter (fn [x]
                                  (empty?
                                    (filter
                                      (fn [y] (= y (first x))) cfws))) workers)

                  fcsts (filter (fn [x]
                                  (empty?
                                    (filter
                                      (fn [y]
                                        (and
                                          (= (get x :id) (get y :id))
                                          (= (get x :sid) (get y :sid))
                                          )
                                        )
                                      csts))) (get @state :subtasks)) ]
              (println @state)
              (set-state! { :workers (concat fcfws (map #(vector % true) fws))
                           :node-id node-id
                           :subtasks fcsts })
              (println "dist:" dist)
              (doseq [[w s] dist]
                (let [id (get s :id)
                      sid (get s :sid)
                      st (get s :subtask)
                      filtered-workers (filter #(not (= w (first %))) workers)
                      current-worker (first (filter #(= w (first %)) workers))]
                  (println "st:" id sid st)
                  (! w [:subtask id sid st])))))
          :else (println "WAT")))
      (recur))))

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
  (let [listening-port (read-string (nth args 1))
        listening-host (nth args 0)
        manager-host (nth args 2)
        manager-port (read-string (nth args 3))
        threads (read-string (nth args 4))
        n (spawn node manager-host manager-port threads listening-port listening-host)
        l-soc (create-server-socket listening-port)
        l (spawn-fiber listener n l-soc)]
    (do
      (join n)
      (join l))))
