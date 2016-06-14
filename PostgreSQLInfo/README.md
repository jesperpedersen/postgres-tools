# PostgreSQLInfo

This tool generates a THML report about the PostgreSQL configuration.

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

PostgreSQLInfo takes an optional 2nd parameter, which is the name of report, f.ex.

```bash
/path/to/work_area
/path/to/java PostgreSQLInfo /path/to/data_directory myreport.html
```

## Report

The reports are located in the '/path/to/work_area' directory

The report show

* Location of data directory
* Location of xlog directory
* Content of ```pg_hba.conf```
* Content of ```postgresql.conf```
