# Replay

This tool replays SQL through JDBC against PostgreSQL.

## Requirements

* Java 8
* PostgreSQL
* PostgreSQL/JDBC
* [JSqlParser](https://github.com/JSQLParser/JSqlParser/wiki)

## Installation

* Compile Replay
* Download the PostgreSQL JDBC driver

### Compile Replay

```bash
cd /path/to/work_area
wget https://oss.sonatype.org/content/groups/public/com/github/jsqlparser/jsqlparser/0.9.6/jsqlparser-0.9.6.jar
/path/to/javac -classpath jsqlparser-0.9.6.jar Replay.java
```

### Download the PostgreSQL JDBC driver

Download from the [PostgreSQL JDBC](https://jdbc.postgresql.org/download.html) web site.

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

and do a run of the SQL statement that you want to replay.

### Generate profile for the run

```bash
cd /path/to/work_area
/path/to/java -classpath .:jsqlparser-0.9.6.jar:postgresql-9.4.1211.jar Replay -i postgresql.log
```

The name of the profile is basename of the log file, e.g. ```postgresql``` in the above example.

### Configuration

The configuration of Replay is done in the ```replay.properties``` file, which supports the following
options.

#### host

The host name of the PostgreSQL server.

Default is ```localhost```

#### port

The port of the PostgreSQL server.

Default is ```5432```

#### database

The name of the database.

Required option.

#### user

The user name to connect with.

Required option.

#### password

The password of the user.

Required option.

### Run

```bash
cd /path/to/work_area
/path/to/java -classpath .:jsqlparser-0.9.6.jar:postgresql-9.4.1211.jar Replay postgresql
```

## Result

The result of the run is displayed in the console.

The result shows

* The clock time
* The number of clients used
* The run time / connection time of each client

Futhermore, a .csv file with the results is created in the profile directory.
