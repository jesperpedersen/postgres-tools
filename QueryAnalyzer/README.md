# QueryAnalyzer

This tool generates HTML reports about queries run against PostgreSQL.

## Requirements

* Java 8
* PostgreSQL
* PostgreSQL/JDBC
* [JSqlParser](https://github.com/JSQLParser/JSqlParser/wiki)

## Installation

* Compile QueryAnalyzer
* Download the PostgreSQL JDBC driver

### Compile QueryAnalyzer

```bash
cd /path/to/work_area
wget https://oss.sonatype.org/content/groups/public/com/github/jsqlparser/jsqlparser/1.0/jsqlparser-1.0.jar
/path/to/javac -classpath jsqlparser-1.0.jar QueryAnalyzer.java
```

### Download the PostgreSQL JDBC driver

Download from the [PostgreSQL JDBC](https://jdbc.postgresql.org/download.html) web site.

## Usage

### Run

```bash
cd /path/to/work_area
/path/to/java -classpath .:postgresql-42.1.1.jar:jsqlparser-1.0.jar QueryAnalyzer
```

## Configuration

The configuration of QueryAnalyzer is done in the ```queryanalyzer.properties``` file, which supports the following
options.

Alternative, Queryanalyzer can be configured through another properties file and executed with

```bash
cd /path/to/work_area
/path/to/java -classpath .:postgresql-42.1.1.jar:jsqlparser-1.0.jar QueryAnalyzer -c myprops.properties
```

### host

The host name of the PostgreSQL server.

Default is ```localhost```

### port

The port of the PostgreSQL server.

Default is ```5432```

### database

The name of the database.

Required option.

### user

The user name to connect with.

Required option.

### password

The password of the user.

Required option.

### plan_count

The number of times the query should be executed before measured.

Default is ```5```

### row_information

Display information about number of rows in each table, and number of index entries per index.

Default is ```false```

### query

Each configuration option that starts with ```query``` is identified as a query that should be analyzed.

Examples

```
query.select.01=SELECT * FROM weather
query.select.02=SELECT * FROM weather_history
```

The query identifiers provides the links to the report in the main report.

## Report

The reports are located in the ```/path/to/work_area/report``` directory. The main report is ```index.html```

Overview reports

* Tables - layout, primary key, existing indexes and suggestions for primary key and indexes
* Times - CSV file with planning and execution times
* HOT - Shows column status: Black = Not updated, Green = Updated but not part of an index, Red = Updated and part of an index

Each query report shows

* The query
* The executed query, if ```query``` contains ```?```
* The ```EXPLAIN (ANALYZE, VERBOSE, BUFFERS ON)``` plan
* The replay file
* Overview of the tables / indexes for the query
