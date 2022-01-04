(ns onyx-stream-sql.core
  (:require [onyx-local-rt.api :as api]
            [clojure.pprint :as pp]))

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 20}
             {:onyx/name :inc
              :onyx/type :function
              :onyx/fn ::my-inc
              :onyx/batch-size 20}
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 20}]
   :lifecycles []})

(pp/pprint
 (-> (api/init job)
     (api/new-segment :in {:n 41})
     (api/new-segment :in {:n 84})
     (api/drain)
     (api/stop)
     (api/env-summary)))

;; =>
;; {:next-action :lifecycle/start-task?,
;;  :tasks
;;  {:in {:inbox []},
;;   :inc {:inbox []},
;;   :out {:inbox [], :outputs [{:n 42} {:n 85}]}}}