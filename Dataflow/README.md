# Dataflow

This tool analyzes the SQL sent to PostgreSQL.

## Requirements

* Java 8
* PostgreSQL
* [JSqlParser](https://github.com/JSQLParser/JSqlParser/wiki)

## Installation

* Compile Dataflow

### Compile Dataflow

```bash
cd /path/to/work_area
wget https://oss.sonatype.org/content/groups/public/com/github/jsqlparser/jsqlparser/0.9.6/jsqlparser-0.9.6.jar
/path/to/javac -classpath jsqlparser-0.9.6.jar Dataflow.java
```

## Usage

### Configure PostgreSQL

Configure PostgreSQL with the following settings in ```postgresql.conf```

```
log_destination = 'stderr'
log_directory = 'pg_log'
log_filename = 'postgresql.log'
log_rotation_age = 0
log_min_duration_statement = 0
log_line_prefix = '%p [%t] [%x] '
```

and do a run of the SQL statement that you want to analyze.

### Run

```bash
cd /path/to/work_area
/path/to/java -classpath .:jsqlparser-0.9.6.jar Dataflow postgresql.log
```

## Result

The result of the analysis will be generated in the ```report``` directory.
The main report is called ```index.html```.

The report will highlight the following issues.

### UPDATE

Rule 1:
```
UPDATE tab SET col1 = 0, col2 = 1 WHERE col1 = 0
```

Rule 2:
```
BEGIN
UPDATE tab SET col2 = 1 WHERE col1 = 0
UPDATE tab SET col2 = 1 WHERE col1 = 0
COMMIT
```
