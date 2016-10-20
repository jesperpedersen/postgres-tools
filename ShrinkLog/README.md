# ShrinkLog

This tool can shrink a PostgreSQL log file.

## Requirements

* Java 8
* PostgreSQL

## Installation

* Compile ShrinkLog

### Compile ShrinkLog

```bash
cd /path/to/work_area
/path/to/javac ShrinkLog.java
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

and do a run of the SQL statement that you are interested in.

### Run

```bash
cd /path/to/work_area
/path/to/java ShrinkLog <log_file> [max_statements]
```

where ```<log_file>``` is the log file (```postgresql.log```), and ```[max_statements]```
is an optional parameter that specifies how many SQL statements a single client
should execute at maximum.

## Result

The result of the command is in ```output.log``` file.
