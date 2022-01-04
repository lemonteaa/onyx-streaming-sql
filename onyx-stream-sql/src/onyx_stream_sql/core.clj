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

(defn add-segment [env [input-name segment]]
  (api/new-segment env input-name {:src input-name
                                   :data segment}))

(defn runtest [job segments]
  (let [env (api/init job)
        nextenv (reduce add-segment
                        env segments)]
    (-> nextenv
        (api/drain)
        (api/stop)
        (api/env-summary))))
;; =>
;; {:next-action :lifecycle/start-task?,
;;  :tasks
;;  {:in {:inbox []},
;;   :inc {:inbox []},
;;   :out {:inbox [], :outputs [{:n 42} {:n 85}]}}}

(def test1
  {:select [:b/age :b/name]
   :from [:person :b]
   :where [:age-large]})

(def test2
  {:select [:b/age :b/name :c/country-name]
   :from [:person :b]
   :join {:target [:country :c]
          :on-key [:c/country-code :b/country]}
   :where [:age-large]})

(defn gen-input [name]
  {:onyx/name name
   :onyx/type :input
   :onyx/batch-size 20})

(defn gen-output [name]
  {:onyx/name name
   :onyx/type :output
   :onyx/batch-size 20})

(defn run-process [sql segment]
  (let [ks (map #(keyword (name %)) (:select sql))]
    (select-keys (:data segment) ks)))

(defn compile-query [sql]
  (let [input-name (first (:from sql))
        output-name :out
        has-join? (contains? sql :join)
        join-name (if has-join? 
                    (-> sql
                        (get-in [:join :target])
                        (first)))]
    {:workflow (cond-> []
                 true (conj [input-name :proc])
                 has-join? (conj [join-name :proc])
                 true (conj [:proc output-name]))
     :catalog (cond-> []
                true (conj (gen-input input-name))
                has-join? (conj (gen-input join-name))
                true (conj {:onyx/name :proc
                            :onyx/type :function
                            :onyx/fn ::run-process
                            :streamql/sql sql
                            :onyx/params [:streamql/sql]
                            :onyx/batch-size 20})
                true (conj (gen-output output-name)))
     :lifecycles []}))

(runtest
 (compile-query test1)
 [[:person {:age 34 :name "Mary" :other true :test 123}]
  [:person {:age 67 :name "Peter" :other false :test 0}]
  [:person {:age 21 :name "Hackle" :other true}]
  [:person {:age 18 :name "Oliver" :other false :foo :bar}]])
