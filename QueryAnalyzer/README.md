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
wget https://oss.sonatype.org/content/groups/public/com/github/jsqlparser/jsqlparser/1.4/jsqlparser-1.4.jar
/path/to/javac -classpath jsqlparser-1.4.jar QueryAnalyzer.java
```

### Download the PostgreSQL JDBC driver

Download from the [PostgreSQL JDBC](https://jdbc.postgresql.org/download.html) web site.

## Usage

### Run

```bash
cd /path/to/work_area
/path/to/java -classpath .:postgresql-42.2.5.jar:jsqlparser-1.4.jar QueryAnalyzer
```

## Configuration

The configuration of QueryAnalyzer is done in the ```queryanalyzer.properties``` file, which supports the following
options.

Alternative, Queryanalyzer can be configured through another properties file and executed with

```bash
cd /path/to/work_area
/path/to/java -classpath .:postgresql-42.2.5.jar:jsqlparser-1.4.jar QueryAnalyzer -c myprops.properties
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

### show_partitions

Show partitions in the "Tables" report.

Default is ```false```

### issues

Show potential issues with the query.

Default is ```true```

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

The main report shows

* Time of the run
* Links to detailed reports
* Queries with identifier, planning time, execution time and planner node types

Overview reports

* Tables - layout, primary key, existing indexes and suggestions for primary key and indexes
* Indexes - Show index usage, and unused indexes
* HOT - Shows column status: Black = Not updated, Green = Updated but not part of an index, Red = Updated and part of an index
* Times - CSV file with planning and execution times
* Suggestions - SQL with possible optimizations, as suggested in the 'Tables' report
* Environment - Shows the PostgreSQL environment

Each query report shows

* The query
* The executed query, if ```query``` contains ```?```
* The ```EXPLAIN (ANALYZE, VERBOSE, BUFFERS ON)``` plan
* The replay file
* Any issues, like duplicated columns
* Overview of the tables, indexes and foreign key constraints for the query
