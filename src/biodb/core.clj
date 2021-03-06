(ns biodb.core
  (:require [clojure.java.jdbc :refer :all]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [taoensso.nippy :as nip]
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

(defn freeze
  "Uses the Nippy library to serialise a data structure."
  [obj]
  (nip/freeze obj))

(defn thaw
  [obj]
  "Deserialises an object frozen with the freeze function."
  (nip/thaw obj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti table-spec
  "Returns a vector of vectors specifying a table spec for creating
  tables using clojure.java.jdbc. For binary fields returns a spec
  with the :binary keyword instead of database specific data
  type (biodb sort this out itself)."
  (fn [q] (:type q)))
(defmulti prep-sequences
  "Returns collection of hashmaps representing rows in the database."
  (fn [q] (:type q)))
(defmulti restore-sequence
  "Takes a database row and returns a hashmap representing the
  biological sequence."
  (fn [q] (:type q)))

(defmethod table-spec :default
  [q]
  (vector [:accession :text "PRIMARY KEY"]
          [:src :text "NOT NULL"]))

(defmethod prep-sequences :default
  [q]
  (map #(hash-map :accession (:accession %) :src (pr-str %)) (:coll q)))

(defmethod restore-sequence :default
  [q]
  (edn/read-string (:src q)))

(defn db-spec
  "Takes a map of parameters and returns a database spec. Currently
  sqlite and postgres are supported and which one should be specfified
  in the :dbtype argument as :postgres or :sqlite."
  [{:keys [dbname dbtype user password port domain]
    :or {port "5432" domain "127.0.0.1"}
    :as params}]
  (check-spec-args params)
  (let [spec (condp = dbtype
               :postgres (let [sn (str "//" domain ":" port "/" dbname)]
                           (assoc (pool/make-datasource-spec
                                   {:classname "org.postgresql.Driver"
                                    :subprotocol "postgresql"
                                    :subname sn
                                    :user user
                                    :password password})
                                  :dbtype :postgres))
               :sqlite {:classname   "org.sqlite.JDBC"
                        :subprotocol "sqlite"
                        :subname     dbname
                        :dbtype :sqlite})]
    spec))

(defn- binary-fields
  [db type]
  (let [r (table-spec {:type type})
        bin (condp = (:dbtype db) :postgres :bytea :sqlite :blob)]
    (into [] (map #(if (= :binary (nth % 1))
                     (assoc % 1 bin)
                     %))
          r)))

(defn create-table!
  "Given a database spec, table name and a type creates a table in the
  database. Type argument is used to dispatch the table-spec argument
  and a method must be defined for the type. Type argument refers to
  the dispatch argument of the multimethods table-spec, prep-sequences
  and restore-sequences. See clj-fasta for an example of these
  methods."
  [db table type]
  (let [t (if (keyword? table) (name table) table)]
    (db-do-commands db (create-table-ddl t (binary-fields db type)))))

(defn insert-sequences!
  "Given a database spec, table name, sequence type and a collection
  of sequences, will insert sequences into the specified table. Type
  argument refers to the dispatch argument of the multimethods
  table-spec, prep-sequences and restore-sequences. See clj-fasta for
  an example of these methods."
  ([db table type coll] (insert-sequences! db table type coll false))
  ([db table type coll return]
   (let [a (insert-multi! db table (prep-sequences {:coll coll :type type}) {:transaction? true})]
     (if return a true))))

(defn- large-query
  [t tt {:keys [apply-func where select join offset order limit parameters]
         :or {apply-func nil where nil select nil join nil
              offset nil order nil limit nil parameters nil}}]
  (-> (cons (str "select "
                 (if select
                   (->> (map name select) (interpose ",") (apply str))
                   "*")
                 " from " t " "
                 (if join join)
                 " inner join " tt " on " t ".accession=" tt ".accession"
                 (if where (str " where " where))
                 (if order (str " order by " (name order)))
                 (if offset " offset ?")
                 (if limit " limit ? "))
            (concat parameters (->> [(or offset)
                                     (or limit)]
                                    (remove nil?))))
      vec))

(defn- small-query
  [t coll {:keys [apply-func where select join offset order limit parameters]
           :or {apply-func nil where nil select nil join nil
                offset nil order nil limit nil parameters nil}}]
  (-> (cons (str "select "
                 (if select
                   (->> (map name select) (interpose ",") (apply str))
                   "*")
                 " from " t " "
                 (if join join)
                 " where " t ".accession in ("
                 (->> (repeat (count coll) "?") (interpose ",") (apply str))
                 ")"
                 (if where (str " and " where))
                 (if order (str " order by " (name order)))
                 (if offset " offset ?")
                 (if limit " limit ?"))
            (concat coll parameters (->> [(or offset)
                                          (or limit)]
                                         (remove nil?))))
      vec))

(defn get-sequences
  "Given a database spec, table name, sequence type and a collection
  of accessions, will return corresponding sequences from the
  database. Type argument refers to the dispatch argument of the
  multimethods table-spec, prep-sequences and restore-sequences. See
  clj-fasta for an example of these methods."
  [db table type coll & {:keys [apply-func where select join offset order limit parameters]
                         :or {apply-func nil where nil select nil join nil
                              offset nil order nil limit nil parameters nil}
                         :as m}]
  (if (> (count coll) 100)
    (let [t (name table)
          tt (str (gensym))
          qu (large-query t tt m)]
      (try
        (with-db-transaction [con db]
          (condp = (:dbtype db)
            :postgres (let [tfile (fs/temp-file "ids")]
                        (with-open [w (io/writer tfile)] (doseq [i (set coll)] (.write w (str i "\n"))))
                        (execute! con [(str "create temp table " tt " (accession text) on commit drop")])
                        (execute! con [(str "copy " tt " from '" tfile "'")]))
            :sqlite (do
                      (execute! con [(str "create temp table " tt " (accession text)")])
                      (insert-multi! con tt (map #(hash-map :accession %) (set coll)))))
          (query con qu (if-not apply-func
                          {:row-fn #(restore-sequence (assoc % :type type))}
                          {:row-fn #(restore-sequence (assoc % :type type))
                           :result-set-fn apply-func})))))
    (let [t (if (keyword? table) (name table) table)
          qu (small-query t coll m)]
      (query db qu (if-not apply-func
                     {:row-fn #(restore-sequence (assoc % :type type))}
                     {:row-fn #(restore-sequence (assoc % :type type))
                      :result-set-fn apply-func})))))

(defn query-sequences
  "Given a database spec, query vector and sequence type, will return
  sequences from the database. Does not return a lazy list. Type
  argument refers to the dispatch argument of the multimethods
  table-spec, prep-sequences and restore-sequences. See clj-fasta for
  an example of these methods."
  [db q type & {:keys [apply-func where select join offset order limit parameters]
                :or {apply-func nil where nil select nil join nil
                     offset nil order nil limit nil parameters nil}}]
  (query db q (if-not apply-func
                {:row-fn #(restore-sequence (assoc % :type type))}
                {:row-fn #(restore-sequence (assoc % :type type))
                 :result-set-fn apply-func})))

(defn do-command
  "Performs a general (non-select) sql command. Takes a vector
  containing sql and optional parameters and options map. Thin wrapper
  around JDBC execute!  command."
  ([db q] (do-command db q nil))
  ([db q opts]
   (execute! db q opts)))

(defn close-pooled-connection
  "Closes a pooled database such as is created when using postgres."
  [db]
  (.close (:datasource db)))

(defmacro with-transaction
  "Perform multiple operations within a transaction."
  [binding & body]
  `(with-db-transaction ~binding ~@body))
