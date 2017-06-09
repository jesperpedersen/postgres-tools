# LWLockStats

This tool generates input files to [flamegraph](https://github.com/brendangregg/FlameGraph)
from the [lwstat](https://github.com/jesperpedersen/postgres/tree/lwstat) branch.

## Requirements

* Java 8
* PostgreSQL lwstat branch
* FlameGraph

## Installation

* Compile LWLockStats
* Compile PostgreSQL lwstat branch
* Get FlameGraph

### Compile LWLockStats

```bash
cd /path/to/work_area
/path/to/javac LWLockStats.java
```

### Compile PostgreSQL lwstat branch

```bash
cd /path/to/work_area
git clone https://github.com/jesperpedersen/postgres.git
cd postgres
git checkout lwstat
export CFLAGS="-DLWLOCK_STATS -DLWLOCK_STATS_QUEUE_SIZES" && ./configure ...
make
make install
```

### Get FlameGraph

```bash
cd /path/to/work_area
git clone https://github.com/brendangregg/FlameGraph.git
```

## Usage

### PostgreSQL

The captured LWLock information will be outputted in the log upon shutdown of
the server.

### Run

```bash
cd /path/to/work_area
/path/to/java -Xmx64g LWLockStats postgresql.log
```

Note, that LWLockStats requires a lot of memory to process the log file, so adjust the
```-Xmx64g``` parameter to account for your maximum memory size.

Alternative, you can combined all LWLock type information under a single entry

```bash
cd /path/to/work_area
/path/to/java -Xmx64g LWLockStats -c postgresql.log
```

This requires less memory.

## Report

The input files are created in the same directory, and are
* ```weight.txt```: The weight of the LWLock; calculated as ```10 * exclusive locks + 1 * shared locks```
* ```exclusive.txt```: The maximum chain of exclusive locks on the LWLock instance
* ```shared.txt```: The maximum chain of shared locks on the LWLock instance
* ```block.txt```: The blocks for the LWLock instance
* ```spin.txt```: The spins for the LWLock instance

The flamegraph can be created using

```bash
cd /path/to/work_area
FlameGraph/flamegraph.pl --title='Weight' weight.txt > weight.svg
```

and so on.
