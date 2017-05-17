# CronRun

This tool is setup to do a daily run against the PostgreSQL master branch in order to
test the performance.

## Requirements

* Java 8
* Compiler environment (gcc and make)
* git
* bash
* sed

## Installation

* Copy the perf.sh script to the machine doing the runs
* Edit the perf.sh variables
* Clone the PostgreSQL Git repository
* Setup cron job
* Get DyGraph
* Create machine specific files
* Generate the report

### Copy the perf.sh script to the machine doing the runs

```bash
cp perf.sh /path/to/work_area
```

### Edit the perf.sh variables

```bash
cd /path/to/work_area
emacs perf.sh
```

### Clone the PostgreSQL Git repository

```bash
cd /path/to/work_area
git clone https://github.com/postgres/postgres.git
```

### Setup cron job

```bash
cd /path/to/work_area
crontab -e
```

### Get DyGraph

```bash
cd /path/to/work_area
wget http://dygraphs.com/2.0.0/dygraph.min.js
wget http://dygraphs.com/2.0.0/dygraph.min.css
```

### Create machine specific files

You can create a ```machine.html``` which is displayed on the front page to highlight your machine setup.

You can create a ```footer.html``` which is displayed as the footer for each of the HTML pages generated.

Both files just needs to contain a HTML fragment, as they are included in the generated page.

### Generate the report

```bash
cd /path/to/work_area
/path/to/java CronRun
```

Compile with ```javac``` before use.

## Configuration

The perf.sh scripts contains the following configuration options

### CLIENTS

The list of client counts for the run

### TCP

Determines if the run should be done with Unix Domain Sockets (```0```) or
with TCP/IP (```1```)

### HOST

The name of the host for ```TCP=1```

### PORT

The port used for ```TCP=1```

### SCALE

The scale factor for pgbench

### TIME

The number of seconds for each run

### PGSQL_ROOT

The root of the PostgreSQL installation

### PGSQL_DATA

The location of the PostgreSQL data directory

### PGSQL_XLOG

The location of the PostgreSQL xlog directory

### COMPILE_OPTIONS

The compiler options passed to configure

### COMPILE_JOBS

The number of compiler jobs

### CONFIGURATION

The location of the PostgreSQL configuration template for the runs

### PATCHES

The location of the directory where patches should be applied from

### RUN_TWOPC

Should two-phase commit scenarios be executed.

Require patch from https://github.com/jesperpedersen/postgres/tree/pgbench_xa

## Report

The report is located in the 'report' directory with the top-level file of index.html

### Daily reports

Shows the graphs for the daily run as well as

* Commit identifier
* Environment
* Configuration
* WAL information
* Note for the run (loaded from notes.properties)
* If a run should be ignored (loaded from ignores.properties)

### Profile reports

Shows the graphs for each profile

### Max reports

Shows the maximum transaction per second (TPS) for each profile
