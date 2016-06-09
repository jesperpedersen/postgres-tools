# PerfRun

This tool executes runs with Linux "perf record", and generate flamegraphs.

## Requirements

* Java 8
* PostgreSQL
* Linux perf
* sed
* perl
* [FlameGraph](https://github.com/brendangregg/FlameGraph)

## Installation

* Compile PostgreSQL
* Compile PerfRun
* Get FlameGraph

### Compile PostgreSQL

```bash
export CFLAGS="-O -fno-omit-frame-pointer" && ./configure --prefix /opt/postgresql-9.6
```

The two-phase commit report requires https://github.com/jesperpedersen/postgres/tree/pgbench_xa

### Compile PerfRun

```bash
/path/to/work_area
/path/to/javac PerfRun.java
```

### Get FlameGraph

```bash
/path/to/work_area
wget https://raw.githubusercontent.com/brendangregg/FlameGraph/master/stackcollapse-perf.pl
wget https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl
```

## Usage

### Run

```bash
/path/to/work_area
/path/to/java PerfRun
```

## Configuration

The PerfRun program can be configured through the ```perfrun.properties``` file, which
supports the following configuration options

Format is ```key=value```

### PGSQL_ROOT

The root of the PostgreSQL installation

Default is ```/opt/postgresql-9.6```

### PGSQL_DATA

The location of the PostgreSQL data directory

Default is ```/mnt/data/9.6```

### PGSQL_XLOG

The location of the PostgreSQL xlog directory

Default is ```/mnt/xlog/9.6```

### CONFIGURATION

The location of the PostgreSQL configuration template for the runs

Default is ```/home/postgres/Configuration/9.6```

Note, that the template should contain ```synchronous_commit = off``` as the default value

### USER_NAME

The user name to run under

Default is ```postgres```

### CONNECTIONS

The number of connections used for the run

Default is ```100```

### SCALE

The scale factor for pgbench

Default is ```3000```

### RUN_SECONDS

The number of seconds for each run

Default is ```300```

### SAMPLE_RATE

The number of milliseconds between the samples in perf record

Default is ```197```

### PERF_RECORD_SLEEP

The number of milliseconds to sleep after stopping the perf record process

Default is ```60000``` - e.g. 1 min

### RUN_TWOPC

Should two-phase commit scenarios be executed.

Default is ```true```

## Report

The reports are located in the 'report' directory.

The directory contains

* The flamegraph for each of the profile runs
* The stack collapsed file (compressed) for each of the profile runs

Check out [Brendan Gregg](http://www.brendangregg.com/flamegraphs.html)'s web site
for more information on [FlameGraph](https://github.com/brendangregg/FlameGraph)s
