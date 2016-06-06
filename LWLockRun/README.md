# LWLockRun

This tool analyze the PostgreSQL LWLock usage, and generate flamegraphs.

## Requirements

* Java 8
* PostgreSQL
* sed
* perl
* [FlameGraph](https://github.com/brendangregg/FlameGraph)

## Installation

* Compile PostgreSQL
* Compile LWLockRun
* Get FlameGraph

### Compile PostgreSQL

```bash
export CFLAGS="-DLWLOCK_STATS -DLWLOCK_STATS_QUEUE_SIZES" && ./configure --prefix /opt/postgresql-9.6
```

The patch can be found at https://github.com/jesperpedersen/postgres/tree/lwstat

The two-phase commit report requires https://github.com/jesperpedersen/postgres/tree/pgbench_xa

### Compile LWLockRun

```bash
/path/to/work_area
/path/to/javac LWLockRun.java
```

### Get FlameGraph

```bash
/path/to/work_area
wget https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl
```

## Usage

### Run

```bash
/path/to/work_area
/path/to/java LWLockRun
```

## Configuration

The LWLockRun program can be configured through the ```lwlockrun.properties``` file, which
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

### SLEEP_TIME

The number of milliseconds to sleep while PostgreSQL shuts down

Default is ```300000``` - e.g. 5 mins

### RUN_TWOPC

Should two-phase commit scenarios be executed.

Default is ```true```

## Report

The reports are located in the 'report' directory

Each report shows the lock queue depth on the Y-axis, and width of each data point is specific to the graph

### Weight

The weight graph, which is calculated as 10 x exclusive lock + shared lock

### Exclusive

The exclusive lock graph

### Shared

The shared lock graph

### Block

The block count graph

### Spin

The spin count graph

