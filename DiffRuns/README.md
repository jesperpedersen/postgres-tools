# DiffRuns

This tool compares two performance runs against each other, and output a CSV file with the results.

## Requirements

* Java 8

## Installation

* Get PostgreSQL
* Compile DiffRuns

### Get PostgreSQL

```bash
cd /path/to/work_area
git clone https://github.com/postgres/postgres.git
```

The two-phase commit report requires https://github.com/jesperpedersen/postgres/tree/pgbench_xa

### Compile DiffRuns

```bash
cd /path/to/work_area
/path/to/javac DiffRuns.java
```

## Usage

### Generating the data necessary

For ```origCommit``` you can use [CronRun](https://github.com/jesperpedersen/postgres-tools/tree/master/CronRun)
to generate nightly reports to run against. Otherwise you can execute ```run.sh``` for ```origCommit``` too.

For ```patchCommit``` you can run

```bash
cd /path/to/work_area
./run.sh
```

after applying the patch you want to test to the ```postgres``` repository

```bash
cd /path/to/work_area/postgres
git checkout -b patch master
patch -p1 < /path/to/patch
git add ...
git commit -a -m "Patch description"
```

Note, that by default ```run.sh``` will compile and install the active branch.

### Run

The ```work_area``` directory needs to contain all the ```*.txt``` files from both runs.

```bash
cd /path/to/work_area
/path/to/java DiffRuns origCommit patchCommit
```

F.ex.

```bash
/path/to/java DiffRuns d0f2f53cd6f2f1fe6e53b8e3bfcce43c16ea851b 978b2f65aa1262eb4ecbf8b3785cb1b9cf4db78e
```

## Report

The report is located in the 'report' directory under the ```patchCommit.csv``` file name.

This data is then easy viewed and presented in an application like
[LibreOffice](https://www.libreoffice.org/)
