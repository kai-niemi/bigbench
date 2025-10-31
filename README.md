[![Java CI with Maven](https://github.com/kai-niemi/bigbench/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/kai-niemi/bigbench/actions/workflows/maven.yml)
       
<!-- TOC -->
* [About BigBench](#about-bigbench)
* [How it works](#how-it-works)
* [Quick Setup](#quick-setup)
* [Usage](#usage)
  * [IMPORT INTO using CSV or Avro OCF streams over HTTP](#import-into-using-csv-or-avro-ocf-streams-over-http)
  * [COPY using CSV or Avro OCF streams over HTTP](#copy-using-csv-or-avro-ocf-streams-over-http)
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

<img align="left" src="logo.png" width="128" /> A benchmarking tool for 
CockroachDB bulk data ingestion. It provides the most common bulk ingestion 
methods for CockroachDB, including:

1. `IMPORT INTO` using CSV or Avro OCF streams over HTTP
2. `COPY` using CSV or Avro OCF streams over HTTP
3. `INSERT` batch statements with array unnesting
4. `INSERT` batch statements with driver multi-value rewrites
5. `INSERT` singleton statements

The best performance is typically achieved using the method in 
top-down order of the above.  

# How it works

It works by generating random data streams over HTTP, consumed either by 
the app itself or by CockroachDB through the use of `COPY` and `IMPORT` 
methods. The data streams are table centric and based either on table
schema introspection or custom layouts. 

# Quick Setup

See the [building](#building) section for prerequisites.

Create the database:

    cockroach sql --insecure --host=localhost -e "CREATE DATABASE bigbench"

Load a sample schema:

    cockroach sql --insecure --host=localhost --database bigbench < samples/create-default.sql

# Usage

## IMPORT INTO using CSV or Avro OCF streams over HTTP
                                                   
This method involves issuing the `IMPORT INTO` command that consumes either a CSV
stream or Avro OCF stream from a BigBench API endpoint. All imports are on a per-table
basis. In this example, we use the `customer` table with its schema defined in [create-default.sql](samples/create-default.sql).

First start the app providing the streaming endpoints:

    ./start.sh

Check that its running and can access the database:

    curl http://localhost:9090/public/customer.csv?rows=10
                      
The API index root is http://localhost:9090/.
                                    
Create an `IMPORT INTO` SQL file for each format with 10K rows:
        
    curl --output work/customer-csv.sql http://localhost:9090/public/customer/csv/import-into.sql?rows=10K
    curl --output work/customer-avro.sql http://localhost:9090/public/customer/avro/import-into.sql?rows=10K

Because the `IMPORT INTO` command take tables offline we can't use introspection to read the schema. Instead, 
we preload the table schema on the server (in-memory):

    curl --output work/customer-csv.json http://localhost:9090/public/customer.csv/form?rows=10K
    curl --output work/customer-avro.json http://localhost:9090/public/customer.avro/form?rows=10K

Optionally, you can edit the json files above to change the row count, column generators or any other details before 
POSTing them back for ephemeral storage:

    curl -d "@work/customer-csv.json" -H "Content-Type:application/json" -X POST http://localhost:9090/public/customer.csv/form
    curl -d "@work/customer-avro.json" -H "Content-Type:application/json" -X POST http://localhost:9090/public/customer.avro/form

Now were all set and can go ahead and start the imports:

    cockroach sql --insecure --host=localhost --database bigbench < work/customer-csv.sql

Should output 60K rows:

```
        job_id        |  status   | fraction_completed | rows  | index_entries |  bytes
----------------------+-----------+--------------------+-------+---------------+-----------
  1120181305635799041 | succeeded |                  1 | 60000 |             0 | 45924963
```

Repeat the same for avro:

    cockroach sql --insecure --host=localhost --database bigbench < work/customer-avro.sql
   
_Hint: If your import jobs get stuck you can cancel them using this command:_

    CANCEL JOBS (WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE job_type='IMPORT' and status not in('failed','succeeded'))
    SELECT * FROM [show jobs] WHERE job_type='IMPORT' and status not in ('failed','succeeded');
    
After the import is completed, you can verify that there's data:

    cockroach sql --insecure --host=localhost --database bigbench -e "select count(1) from customer"

## COPY using CSV or Avro OCF streams over HTTP

One alternative to `IMPORT INTO` that takes the tables offline is to use `COPY .. FROM` instead.

First start the app providing the streaming endpoints:

    ./start.sh

Check that its running and can access the database:

    curl http://localhost:9090/public/customer.csv?rows=10

Generate an `IMPORT INTO` SQL file:

    curl --output work/customer-csv.sql http://localhost:9090/public/customer/csv/import-into.sql?rows=10K

Create a credentials file for `cockroach sql`: 

    echo "--insecure --database bigbench" > work/credentials.txt

Create a header file for copying the `customer` table from stdin that will be piped:

    echo "COPY customer FROM STDIN WITH CSV DELIMITER ',' HEADER;" > work/header.csv

Now go ahead and start the import:

    curl http://localhost:9090/public/customer.csv?rows=10K | cat work/header.csv - | cockroach sql $( cat work/credentials.txt )

## INSERT batch statements with array unnesting

tbd

## INSERT batch statements with driver multi-value rewrites

tbd

## INSERT singleton statements

tbd

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

    sudo apt-get install openjdk-21-jdk

MacOS (using sdkman):

    curl -s "https://get.sdkman.io" | bash
    sdk list java
    sdk install java 21.. (pick version)  

## Install CockroachDB

Ubuntu:
- https://www.cockroachlabs.com/docs/v25.3/install-cockroachdb-linux

MacOS:
- https://www.cockroachlabs.com/docs/v25.3/install-cockroachdb-mac

You can run CockroachDB in single node mode or a full self-hosted cluster. 

## Build and Run

Clone the project:

    git clone git@github.com:kai-niemi/bigbench.git && cd bigbench

Build a single, executable JAR:

    ./mvnw clean install

# Terms of Use

This tool is not supported by Cockroach Labs. Use of this tool is entirely at your
own risk and Cockroach Labs makes no guarantees or warranties about its operation.

See [MIT](LICENSE.txt) for terms and conditions.
