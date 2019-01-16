# SQLLoadGenerator

This tool generates an Online Transaction Processing (OLTP) workload to be used
with [Replay](https://github.com/jesperpedersen/postgres-tools/tree/master/Replay).

## Requirements

* Java 8

## Installation

* Compile SQLLoadGenerator

### Compile SQLLoadGenerator

```bash
cd /path/to/work_area
/path/to/javac SQLLoadGenerator.java
```

## Usage

### Run

```bash
cd /path/to/work_area
/path/to/java SQLLoadGenerator
```

SQLLoadGenerator supports the following options

* `-s`: Scale factor for number of rows (double)
* `-t`: Scale factor for number of statements (double)
* `-c`: Name of the profile that should be used (string)

## Configuration

The configuration of SQLLoadGenerator is done in the ```sqlloadgenerator.properties``` file, which supports the following
options.

Alternative, SQLLoadGenerator can be configured through another properties file and executed with

```bash
cd /path/to/work_area
/path/to/java SQLLoadGenerator -c myprops.properties
```

### clients

The number of clients that should be generated. Default is ```10```.

### client.X.statements

The number of statements for a specific client. Example

```
client.1.statements=10
```

### client.X.mix.select

The mix of ```SELECT``` for a specific client. Example

```
client.1.mix.select=90
```

### client.X.mix.update

The mix of ```UPDATE``` for a specific client. Example

```
client.1.mix.update=5
```

### client.X.mix.insert

The mix of ```INSERT``` for a specific client. Example

```
client.1.mix.insert=5
```

### client.X.mix.delete

The mix of ```DELETE``` for a specific client. Example

```
client.1.mix.delete=0
```

### client.X.commit

The mix of ```COMMIT``` for a specific client. Example

```
client.1.commit=70
```

### client.X.rollback

The mix of ```ROLLBACK``` for a specific client. Example

```
client.1.rollback=30
```

### rows

The global number of rows in each table. Default is ```1000```.

### statements

The number of statements for each client. Default is ```10000```.

### mspt

The maximum number of statements per transaction. Default is ```5```.

### mix.select

The global mix for ```SELECT``` statements.  Default is ```70```.

### mix.select.index

The global mix for ```SELECT``` statements using the defined indexes.  Default is ```0```.

### mix.update

The global mix for ```UPDATE``` statements.  Default is ```15```.

### mix.insert

The global mix for ```INSERT``` statements.  Default is ```10```.

### mix.delete

The global mix for ```DELETE``` statements.  Default is ```5```.

### commit

The global mix for ```COMMIT``` transactions.  Default is ```100```.

### rollback

The global mix for ```ROLLBACK``` transactions.  Default is ```0```.

### notnull

The global `NOT NULL` target. Default is ```100```.

### partitions

The global number of partitions that should be used for each table. Default is ```0```.

### table.X

Define a table called X. Example

```
table.test=This is test table
```

### X.column.Y

Define a column number Y for table X. Example

```
test.column.1=a
```

Column 1 is the default table identifier if no primary key is defined.

### X.column.Y.type

Define the column type for column number Y in table X. Example

```
test.column.1.type=text
```

Default data type is ```int```.

### X.column.Y.description

Define the column description for column number Y in table X. Example

```
test.column.1.description=This is my column
```

### X.column.Y.primarykey

Define the column as the primary key for column number Y in table X. Example

```
test.column.1.primarykey=true
```

### X.column.Y.foreignkey.table

Define the column Y in table X as a foreign key to another table. Example

```
test.column.1.foreignkey.table=anothertable
```

Must be used together with `X.column.Y.foreignkey.column`.

### X.column.Y.foreignkey.column

Define the column Y in table X as a foreign key to another table's column. Example

```
test.column.1.foreignkey.column=id
```

Must be used together with `X.column.Y.foreignkey.table`, and must reference
the table's primary key or leading column.

### X.column.Y.notnull

Define the column as `NOT NULL` for column number Y in table X. Example

```
test.column.2.notnull=true
```

### X.column.Y.unique

Define the column as `UNIQUE` for column number Y in table X. Example

```
test.column.3.unique=true
```

### X.rows

The number of rows for table X. Example

```
test.rows=1000000
```

### X.notnull

The `NOT NULL` target for the table. Default is ```100```.

### X.partitions

The number of partitions that should be used for this table. Example

```
test.partitions=64
```

### X.mix.select

The mix of ```SELECT``` for a specific table. Example

```
test.mix.select=40
```

### X.mix.select.index

The mix of ```SELECT``` for a specific table using its defined indexes. Example

```
test.mix.select.index=20
```

### X.mix.update

The mix of ```UPDATE``` for a specific table. Example

```
test.mix.update=20
```

### X.mix.insert

The mix of ```INSERT``` for a specific table. Example

```
test.mix.insert=40
```

### X.mix.delete

The mix of ```DELETE``` for a specific table. Example

```
test.mix.delete=0
```

### index.X.Y

Define an index for table X. Example

```
index.test.1=a, b
```

## Workload

The workload is located in the ```/path/to/work_area/sqlloadgenerator``` directory, or in the directory
named after the specified property file name.

The workload can then be executed using [Replay](https://github.com/jesperpedersen/postgres-tools/tree/master/Replay).

In order to setup the DDL and data for the workload execute:

```
createuser -P username
createdb -E UTF8 -O username test
psql -U username -f sqlloadgenerator/ddl.sql test
psql -U username -f sqlloadgenerator/data.sql test
```

and then proceed with the [Replay](https://github.com/jesperpedersen/postgres-tools/tree/master/Replay) setup.

A [QueryAnalyzer](https://github.com/jesperpedersen/postgres-tools/tree/master/QueryAnalyzer) file is generated
as well under the name `sqlloadgenerator-queryanalyzer.properties`.
