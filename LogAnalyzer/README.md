# LogAnalyzer

This tool generates HTML reports about queries run against PostgreSQL.

You may want to take a look at [pgBadger](http://dalibo.github.io/pgbadger/) too.

## Requirements

* Java 8
* PostgreSQL

## Installation

* Compile LogAnalyzer
* Get DyGraph

### Compile LogAnalyzer

```bash
cd /path/to/work_area
/path/to/javac LogAnalyzer.java
```

### Get DyGraph

```bash
cd /path/to/work_area
wget http://dygraphs.com/1.1.1/dygraph-combined.js
```

## Usage

### Configure PostgreSQL

Configure PostgreSQL with the following settings in ```postgresql.conf```

```
log_destination = 'stderr'
logging_collector = on
log_directory = 'pg_log'
log_filename = 'postgresql.log'
log_rotation_age = 0
log_min_duration_statement = 0
log_line_prefix = '%p [%m] [%x] '
```

and do a run of the SQL statement that you want to analyze.

### Configuration

The configuration of LogAnalyzer is done in the ```loganalyzer.properties``` file, which supports the following
options.

#### keep_raw

Keep the raw log files

Default is ```false```

#### interaction

Create the interaction reports

Default is ```true```

#### histogram

Create the histogram and time line reports

Default is ```1000```. Use ```0``` for off

#### date_format

The date format definition

Default is ```yyyy-MM-dd HH:mm:ss.SSS```

### Run

```bash
cd /path/to/work_area
/path/to/java LogAnalyzer postgresql.log
```

## Report

The report is located in the ```/path/to/work_area/report``` directory. The main report is ```index.html```

The report shows

* The distribution of ```SELECT```, ```UPDATE```, ```INSERT``` and ```DELETE```
* The times spent in ```PARSE```, ```BIND``` and ```EXECUTE``` phase
* The number of executed queries, and the query itself
* The total and maximum time spent in queries (top 20)
* The interaction of each backend with time and transaction information
* Histogram and time line of each query
