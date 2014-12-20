(ns framework.core)

(defprotocol ClientProtocol
  (sync-call-remote [this ns-name func-name params options])
  (close [this]))
(deftype Client [conn factory content-type options]
  ClientProtocol
  (sync-call-remote [this ns-name func-name params call-options]
    (let [state (get-state factory (server-addr this))
          fname (str ns-name "/" func-name)
          tid (next-trans-id (:idgen state))
          request (make-request tid content-type fname params)
          prms (promise)]
      (swap! (:pendings state) assoc tid {:promise prms})
      (send conn request)
      (deref prms (or (:timeout call-options) (:timeout options) *timeout*) nil)
      (if (realized? prms)
        @prms
        (do
          (swap! (:pendings state) dissoc tid)
          {:cause {:error :timeout}}))))
  (close [this]
    (cancel-ping this)
    (dissoc-client factory this)
    (close conn))

(defn process-call-result [call-result]
  (if (nil? (:cause call-result))
    (:result call-result)))
(defn invoke-remote
  "Invoke remote function with given connection.
   Used exclusevely by defn-remote."
  [sc remote-call-info]
  (let [sc @(or *sc* sc) ;; allow local binding to override client
        [nsname fname args] remote-call-info]
      (process-call-result (sync-call-remote sc nsname fname args options))))

(defmacro defn-remote
  "Define a facade for remote function. You have to provide the
  connection and the function name."
  ([sc fname]
    (let [fname-str (str fname)
          remote-ns-declared (> (.indexOf fname-str "/") 0)
          [remote-ns remote-name] (if remote-ns-declared
                                    (split fname-str #"/" 2)
                                    [remote-ns
                                     (or remote-name fname-str)])
          facade-sym (if remote-ns-declared
                       (symbol remote-name)
                       fname)]
      `(def ~facade-sym
         (with-meta
           (fn [& args#]
             (apply invoke-remote ~sc
               [~remote-ns ~remote-name (into [] args#)]
               (flatten (into [] ~options))))
           {:remote-fn true
            :client ~sc
            :remote-ns ~remote-ns
            :remote-name ~remote-name})))))
