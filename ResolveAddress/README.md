# ResolveAddress

This tool resolves addresses in an input file for flamegraph using eu-addr2line.

## Requirements

* Java 8
* eu-addr2line

## Installation

* Compile ResolveAddress

### Compile ResolveAddress

```bash
/path/to/work_area
/path/to/javac ResolveAddress.java
```

## Usage

### Run

```bash
/path/to/work_area
/path/to/java ResolveAddress /path/to/binary_or_lib file
```

The output is located in a file called ```file.out```
