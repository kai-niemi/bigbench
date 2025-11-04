[![Java CI with Maven](https://github.com/kai-niemi/bigbench/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/kai-niemi/bigbench/actions/workflows/maven.yml)
       
<!-- TOC -->
* [About BigBench](#about-bigbench)
  * [Method Overview](#method-overview)
  * [How it works](#how-it-works)
* [Quick Setup](#quick-setup)
* [Usage](#usage)
  * [IMPORT INTO using CSV or Avro OCF streams over HTTP](#import-into-using-csv-or-avro-ocf-streams-over-http)
  * [COPY using CSV streams over HTTP](#copy-using-csv-streams-over-http)
  * [INSERT batch statements with array unnesting](#insert-batch-statements-with-array-unnesting)
  * [INSERT batch statements with driver multi-value rewrites](#insert-batch-statements-with-driver-multi-value-rewrites)
  * [INSERT singleton statements](#insert-singleton-statements)
* [Building](#building)
  * [Prerequisites](#prerequisites)
  * [Install the JDK](#install-the-jdk)
  * [Install CockroachDB](#install-cockroachdb)
  * [Build and Run](#build-and-run)
* [Terms of Use](#terms-of-use)
<!-- TOC -->

# About BigBench

<img align="left" src="logo.png" width="64"/> 
A benchmarking tool for CockroachDB bulk data ingestion for comparing some  
common bulk ingestion methods. 

## Method Overview

Common ingest methods with respective pros(+) and cons(-), include:

Methods where the database is the client:
* `IMPORT INTO` using CSV or Avro OCF streams over HTTP
  * Best performance (+)
  * Tables are taken offline (-)
* `COPY` using CSV streams over HTTP
  * Tables remain online (+)
  * Not as fast as IMPORT (-)
  
Methods where the tool is the client:
* `INSERT` batch statements with array unnesting
  * Best DML performance (+)
  * Much larger batch sizes (+)
  * Can also do INSERT, UPDATE and UPSERT (+)
  * Involves refactoring in common apps (-)
  * Doesnt work with ORMs (-)
* `INSERT` batch statements with driver multi-value rewrites
  * Non-intrusive, zero effort configuration (+)
  * Only rewrites INSERT, not UPDATE or UPSERT (-)
  * Hard-coded batch size limit (-)
* `INSERT` singleton statements
  * Slowest performance (-)

The best performance is typically achieved with the method in top-down order of the above.  

## How it works

BigBench generates random data streams over HTTP which is consumed either by the tool itself, 
or by CockroachDB through the use of `COPY` and `IMPORT INTO`. The data streams are table 
centric and the schema is derived either from table schema introspection or manually 
configured. 

Since it's stream-oriented it doesnt consume much memory and theres no need to build massive 
CSV or Avro files.

The schema and table selections, batch sizes, row counts, etc. are all configurable. You typically use an
existing database schema of choice and populate the tables with random data. 

# Quick Setup

See the [building](#building) section for prerequisites.

Create the database:

```postgresql
cockroach sql --insecure --host=localhost -e "CREATE DATABASE bigbench"
```

Load a sample schema:

```postgresql
cockroach sql --insecure --host=localhost --database bigbench < samples/create-default.sql
```

# Usage

Quick tutorial of how to use each of the methods above towards a local database.

## IMPORT INTO using CSV or Avro OCF streams over HTTP
                                                   
This method involves issuing the `IMPORT INTO` command that consumes either a CSV
stream or Avro OCF stream from the bigbench API endpoint. All imports are on a per-table
basis. In this example, we use the `customer` table with its schema defined in [create-default.sql](samples/create-default.sql).

First start the server that provides the streaming endpoints for CockroachDB
to use:

```shell
./start.sh
```

The API index root is http://localhost:9090/.

Check that its running and can access the database:

```shell
curl http://localhost:9090/public/customer.csv?rows=10
```

Create an `IMPORT INTO` SQL file with 10,000 rows:

```shell     
curl --output work/customer-csv.sql http://localhost:9090/public/customer/csv/import-into.sql?rows=10K
```

Because the `IMPORT INTO` command take tables offline, we can't use introspection to read the schema so
we need to preload it and store it on the server (stored in-memory).

First get a table schema form (a json document):

```shell
curl --output work/customer-csv.json http://localhost:9090/public/customer.csv/form?rows=10K
```

Optionally, you can edit the json file above to change the row count, column generators or any other details before 
POSTing them back for ephemeral storage.

Post the form back, which will just store it on the server:

```shell
curl -d "@work/customer-csv.json" -H "Content-Type:application/json" -X POST http://localhost:9090/public/customer.csv/form
```

Now go ahead and start the CSV import:

```shell
cockroach sql --insecure --host=localhost --database bigbench < work/customer-csv.sql
```

That should output 60K rows:

```
        job_id        |  status   | fraction_completed | rows  | index_entries |  bytes
----------------------+-----------+--------------------+-------+---------------+-----------
  1120181305635799041 | succeeded |                  1 | 60000 |             0 | 45924963
```

After the import is completed, you can verify that there's data:

```postgresql
cockroach sql --insecure --host=localhost --database bigbench -e "select count(1) from customer"
```

To repeat the same sequence of commands for Avro OCF:

```shell
curl --output work/customer-avro.sql http://localhost:9090/public/customer/avro/import-into.sql?rows=10K
curl --output work/customer-avro.json http://localhost:9090/public/customer.avro/form?rows=10K
curl -d "@work/customer-avro.json" -H "Content-Type:application/json" -X POST http://localhost:9090/public/customer.avro/form
cockroach sql --insecure --host=localhost --database bigbench < work/customer-avro.sql
```

If your import jobs get stuck you can cancel them like this:

```postgresql
CANCEL JOBS (WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE job_type='IMPORT' and status not in('failed','succeeded'))
SELECT * FROM [show jobs] WHERE job_type='IMPORT' and status not in ('failed','succeeded');
```

## COPY using CSV streams over HTTP

One alternative to `IMPORT INTO` is to use `COPY .. FROM`, which doesnt take tables offline.

Start the service as described in the previous section.

Create a header file for copying the `customer` table from `stdin`:

```shell
echo "COPY customer FROM STDIN WITH CSV DELIMITER ',' HEADER;" > work/header.csv
```

Now go ahead and run COPY from `stdin`:

```shell
curl http://localhost:9090/public/customer.csv?rows=10K | cat work/header.csv - | cockroach sql --insecure --database bigbench
```

## INSERT batch statements with array unnesting

This method involves issuing one 
`INSERT INTO .. SELECT unnest (?) as x, unnest (?) as y, .. ON CONFLICT DO NOTHING` statement 
per chuck of CSV rows, where each row is one item in the column `ARRAY`. 

> The `ON CONFLICT DO NOTHING` clause is optional.

Create a command file with the `array-insert` shell command and run:

```shell
echo "array-insert --table customer" > cmd.txt
echo "quit" >> cmd.txt
./run.sh @cmd.txt
```

To see all command options, start the shell and run:

```shell
help array-insert
```

## INSERT batch statements with driver multi-value rewrites

This method involves issuing one
`INSERT INTO .. VALUES (x,y, ..) ON CONFLICT DO NOTHING` statement per chuck of CSV rows 
where each row is one item in a statement batch (`statement.addBatch`).

> The `ON CONFLICT DO NOTHING` clause is optional.

These type of batch statements are however not actually batched over the wire unless 
also `reWriteBatchedInserts` is set to `true`. That in turn rewrite INSERTs to multi-value 
INSERTs but it's also capped to a hardcoded max size of 128.

Create a command file with the `batch-insert` shell command and run:

```shell
echo "batch-insert --table customer" > cmd.txt
echo "quit" >> cmd.txt
./run.sh @cmd.txt
```

To see all command options, start the shell and run:

```shell
help batch-insert
```

## INSERT singleton statements

This method is not recommended for bulk ingest since its very slow, but still here for
comparison.

Create a command file with the `singleton-insert` shell command and run:

```shell
echo "singleton-insert --table customer" > cmd.txt
echo "quit" >> cmd.txt
./run.sh @cmd.txt
```

To see all command options, start the shell and run:

```shell
help singleton-insert
```

# Building

## Prerequisites

- JDK 21 (or later)
  - https://openjdk.org/projects/jdk/21/
  - https://www.oracle.com/java/technologies/downloads/#java21
- CockroachDB 25.2 (or later) 
  - https://www.cockroachlabs.com/docs/releases/
- curl (usually bundled)
  - https://curl.se/

## Install the JDK

Ubuntu:

```shell
sudo apt-get install openjdk-21-jdk
```

MacOS (using sdkman):

```shell
curl -s "https://get.sdkman.io" | bash
sdk list java
sdk install java 21.. (pick version)  
```

## Install CockroachDB

Ubuntu:
- https://www.cockroachlabs.com/docs/v25.3/install-cockroachdb-linux

MacOS:
- https://www.cockroachlabs.com/docs/v25.3/install-cockroachdb-mac

You can run CockroachDB in single node mode or a full self-hosted cluster. 

## Build and Run

Clone the project:

```shell
git clone git@github.com:kai-niemi/bigbench.git && cd bigbench
```

Build a single, executable JAR:

```shell
./mvnw clean install
```

# Terms of Use

This tool is not supported by Cockroach Labs. Use of this tool is entirely at your
own risk and Cockroach Labs makes no guarantees or warranties about its operation.

See [MIT](LICENSE.txt) for terms and conditions.
