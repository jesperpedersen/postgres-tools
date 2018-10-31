# PostgreSQLInfo

This tool generates a HTML report about the PostgreSQL configuration.

## Requirements

* Java 8
* PostgreSQL

## Installation

* Compile PostgreSQLInfo

### Compile PostgreSQLInfo

```bash
/path/to/work_area
/path/to/javac PostgreSQLInfo.java
```

## Usage

### Run

```bash
/path/to/work_area
/path/to/java PostgreSQLInfo /path/to/data_directory
```

PostgreSQLInfo takes an optional 2nd parameter, which is the PostgreSQL major version, f.ex.

```bash
/path/to/work_area
/path/to/java PostgreSQLInfo /path/to/data_directory 9.6
```

which will compare the GUCs against their default. Default version is `11`.

## Report

The reports are located in the ```report/``` directory. The main report is `index.html`.

The report show

* Location of the data directory
* Location of the Write-Ahead Log (WAL) directory
* Content of ```pg_hba.conf```
* Content of ```postgresql.conf```
