# StraceAnalyzer

This tool generates HTML reports from strace information.

## Requirements

* Java 8

## Installation

* Compile StraceAnalyzer

### Compile LogAnalyzer

```bash
cd /path/to/work_area
/path/to/javac StraceAnalyzer.java
```

## Usage

### Get the strace log

The strace log can be obtained by the

```
strace -ttf -o strace.log -p <pid>
```

command. StraceAnalyzer expects this log format.

### Configuration

The configuration of StraceAnalyzer is done in the ```straceanalyzer.properties``` file, which supports the following
options.

#### combine

Combine `<unfinished>` and `<detached>` calls with their counterparts. These calls will be marked with an `[*]` identifier.

Default is ```false```

### Run

```bash
cd /path/to/work_area
/path/to/java StraceAnalyzer strace.log
```

## Report

The report is located in the ```/path/to/work_area/report``` directory. The main report is ```index.html```

The report shows

* The system calls and their number of invocations
* The total number of system call invocations
* A report of each system call with its data
