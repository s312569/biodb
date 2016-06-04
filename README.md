# biodb

Some utilities for storing biological sequences in SQL databases.

## Usage

Install as:

```clj
[biodb "0.1.4"]
```

Include in project:

```clj
(:require [biodb.core :as bdb])
```

Get a database spec for use with the other functions --- currently
sqlite and postgresql are supported and this should be specified as
the :dbtype. If postgresql is being used then also specify a user and
a password. If postgresql :dbname should be the name of an existing
database and for sqlite this shoud be the path to a file, can be
existing if reconnecting to a db or to a new file for a new db:

```clj
user> (def dbspec (bdb/db-spec {:dbname "test-biodb"
                                :dbtype :postgres
                                :user "user"
                                :password "password"}))
#'clj-fasta.core/dbspec
user>
```

To create tables use `create-table!` and provide a db spec, a table
name (can be string or keyword) and a sequence type. The sequence type
is a dispatch value used by the multimethods `table-spec`,
`prep-sequence` and `restore-sequence`. These methods need to be
defined for any sequence type that is to be stored in the
database. `clj-fasta` and `clj-uniprot`, for example, already have
these defined (with the values :fasta and :uniprot). For custom types
just define these multimethods. For example, using the `clj-fasta`
library:

```clj
clj-fasta.core> (bdb/create-table! dbspec :sequences :fasta)
(0)
clj-fasta.core>
```

Insert sequence collections using `insert-sequences!`. Using the
`clj-fasta` library again:

```clj
clj-fasta.core> (def tf "/path/to/file.fasta")
clj-fasta.core> (with-open [r (io/reader tf)]
                  (bdb/insert-sequences! dbspec :sequences :fasta (fasta-seq r)))
nil		  
clj-fasta.core>
```

Get sequences by their accession using `get-sequences` with a
collection of accessions. Note this does not return a lazy collection
and relies on the existence of an 'accession' field.

```clj
clj-fasta.core> (bdb/get-sequences dbspec :sequences :fasta '("Q96QU6"))
({:accession "Q96QU6", :description "1-aminocyclopropane-1-carboxylate
 synthase-like protein 1 [Homo sapiens]", :sequence "MFTLPQK ..."
clj-fasta.core> (bdb/get-sequences dbspec :sequences :fasta '("Q96QU6" "P30483"))
({:accession "Q96QU6", :description "1-aminocyclopropane-1-carboxylate
 synthase-like protein 1 [Homo sapiens]", :sequence "MFTLPQK ..."}
 {:accession "P30483", :description "HLA class I histocompatibility
 antigen, B-45 alpha chain [Homo sapiens]", :sequence "MRVTAP ..."})
clj-fasta.core> 
```

For other queries use `query-sequences`:

```clj
clj-fasta.core> (bdb/query-sequences dbspec
                 		     ["select * from sequences where
				      description like ?" "%aminocyclopropane%"]
				     :fasta)
({:accession "Q96QU6", :description "1-aminocyclopropane-1-carboxylate
 synthase-like protein 1 [Homo sapiens]", :sequence "MFTLPQ ..."}
 {:accession "Q4AC99", :description "Probable inactive 1-aminocyclopropane-1-carboxylate
 synthase-like protein 2 [Homo sapiens]", :sequence "MSHRSDT ..."})
clj-fasta.core> 
```

For lazy processing use `apply-query-sequences` with a function
argument that will work on a lazy list of all sequences retrieved by
the query:

```clj
clj-fasta.core> (bdb/apply-query-sequences dbspec
                 			   ["select * from sequences"]
                 			   :fasta
                 			   #(doall (map :accession %)))
("P31946" "P62258" "Q04917" "P61981" "P31947" ...)
clj-fasta.core>
```

The whole thing is just a very thin wrapper around clojure.java.jdbc
so it is very easy to create novel queries etc.

## License

Copyright © 2016 Jason Mulvenna

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
