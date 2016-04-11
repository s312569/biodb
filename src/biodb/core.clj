(ns biodb.core
  (:require [clojure.java.jdbc :refer :all]
            [jdbc.pool.c3p0 :as pool]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; utilities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- check-spec-args
  [m]
  (if (not (:dbtype m))
    (throw (Exception.
            "Must specify database type (:dbtype). Either :postgres or :sqlite")))
  (if (not (:dbname m))
    (throw (Exception. "Must specify a database name.")))
  (if (and (= (:dbtype m) :postgres)
           (nil? (:user m))
           (nil? (:password m)))
    (throw (Exception. "Must specify a valid user and password."))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti table-spec (fn [q] (:type q)))
(defmulti prep-sequences (fn [q] (:type q)))
(defmulti restore-sequence (fn [q] (:type q)))

(defn db-spec
  "Takes a map of parameters and returns a database spec. Currently
  sqlite and postgres are supported and which one should be specfified
  in the :dbtype argument as :postgres or :sqlite."
  [{:keys [dbname dbtype user password port domain]
    :or {port "5432" domain "127.0.0.1"}
    :as params}]
  (check-spec-args params)
  (condp = dbtype
    :postgres (let [sn (str "//" domain ":" port "/" dbname)]
                (pool/make-datasource-spec
                 {:classname "org.postgresql.Driver"
                  :subprotocol "postgresql"
                  :subname sn
                  :user user
                  :password password}))
    :sqlite {:classname   "org.sqlite.JDBC"
             :subprotocol "sqlite"
             :subname     dbname}))

(defn create-table!
  "Given a database spec, table name and a type creates a table in the
  database. Type argument is used to dispatch the table-spec argument
  and a method must be defined for the type. Type argument refers to
  the dispatch argument of the multimethods table-spec, prep-sequences
  and restore-sequences. See clj-fasta for an example of these
  methods."
  [db table type]
  (let [t (if (keyword? table) (name table) table)]
    (db-do-commands db (create-table-ddl t (table-spec {:type type})))))

(defn insert-sequences!
  "Given a database spec, table name, sequence type and a collection
  of sequences, will insert sequences into the specified table. Type
  argument refers to the dispatch argument of the multimethods
  table-spec, prep-sequences and restore-sequences. See clj-fasta for
  an example of these methods."
  [db table type coll]
  (dorun (->> (prep-sequences {:coll coll :type type})
              (insert-multi! db table))))

(defn get-sequences
  "Given a database spec, table name, sequence type and a collection
  of accessions, will return corresponding sequences from the
  database. Type argument refers to the dispatch argument of the
  multimethods table-spec, prep-sequences and restore-sequences. See
  clj-fasta for an example of these methods."
  [db table type coll]
  (let [t (if (keyword? table) (name table) table)
        qu (str "select * from " t " where accession in ("
                (->> (repeat (count coll) "?") (interpose ",") (apply str))
                ")")]
    (query db (apply vector qu coll)
           {:row-fn #(restore-sequence (assoc % :type type))})))

(defn query-sequences
  "Given a database spec, query vector and sequence type, will return
  sequences from the database. Does not return a lazy list. Type
  argument refers to the dispatch argument of the multimethods
  table-spec, prep-sequences and restore-sequences. See clj-fasta for
  an example of these methods."
  [db q type]
  (query db q
         {:row-fn #(restore-sequence (assoc % :type type))}))

(defn apply-query-sequences
  "Given a database spec, query vector, sequence type and a function,
  will apply specified function to a lazy list of results. Type
  argument refers to the dispatch argument of the multimethods
  table-spec, prep-sequences and restore-sequences. See clj-fasta for
  an example of these methods."
  [db q type func]
  (query db q
         {:row-fn #(restore-sequence (assoc % :type type))
          :result-set-fn func}))

