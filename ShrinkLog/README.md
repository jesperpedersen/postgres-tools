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
logging_collector = on
log_directory = 'pg_log'
log_filename = 'postgresql.log'
log_rotation_age = 0
log_min_duration_statement = 0
log_line_prefix = '%p [%m] [%x] '
```

and do a run of the SQL statement that you are interested in.

### Run

```bash
cd /path/to/work_area
/path/to/java ShrinkLog <log_file> [max_statements] [skip]
```

where ```<log_file>``` is the log file (```postgresql.log```), and ```[max_statements]```
is an optional parameter that specifies how many SQL statements a single client
should execute at maximum. The optional ```skip``` parameter specifies how many statements
or transactions that should be skipped from the beginning of the interaction. If ```skip```
is specified the ```max_statements``` parameter can be set to ```-1``` to keep all other
statements.

## Result

The result of the command is in ```output.log``` file.
