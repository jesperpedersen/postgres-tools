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
wget https://oss.sonatype.org/content/groups/public/com/github/jsqlparser/jsqlparser/1.4/jsqlparser-1.4.jar
/path/to/javac -classpath jsqlparser-1.4.jar Replay.java
```

### Download the PostgreSQL JDBC driver

Download from the [PostgreSQL JDBC](https://jdbc.postgresql.org/download.html) web site.

Alternative JDBC driver: [pgjdbc-ng](http://impossibl.github.io/pgjdbc-ng/).

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
/path/to/java -classpath .:jsqlparser-1.4.jar:postgresql-42.2.5.jar Replay -i postgresql.log
```

The name of the profile is basename of the log file, e.g. ```postgresql``` in the above example.

### Configuration

The configuration of Replay is done in the ```replay.properties``` file, which supports the following
options, as listed below.

All options will be passed to the connection, so all the JDBC driver specific options are supported too.

#### host

The host name of the PostgreSQL server.

Default is ```localhost```

#### port

The port of the PostgreSQL server.

Default is ```5432```

#### database

The name of the database.

*Required option*, if `url` isn't used.

#### url

Specifies the URL that should be used for the connection.

F.ex.

```
url=jdbc:pgsql://localhost:5432/test
```

#### user

The user name to connect with.

*Required option*.

#### password

The password of the user.

*Required option*.

#### max_connections

The maximum number of connections used.

#### quiet

If ```false``` will output status when the tool is preparing for the run.

Default is ```true```.

### Run

```bash
cd /path/to/work_area
/path/to/java -classpath .:jsqlparser-1.4.jar:postgresql-42.2.5.jar Replay postgresql
```

Options:

* `-r`: Iterate through `ResultSet` instances
* `-s`: Run a single client at a time
* `-x`: Use 2-phase semantics for transaction support
* `-e`: Allow exceptions to occur
* `-w`: Wait for user input before starting the run

## Result

The result of the run is displayed in the console.

The result shows

* The clock time
* The number of clients used
* The `run time / connection time / number of statements` of each client, and ` / errors` if enabled 

Futhermore, a .csv file with the results is created in the profile directory.
