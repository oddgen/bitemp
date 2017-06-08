# Bitemp Remodeler for SQL Developer

<img src="https://github.com/oddgen/bitemp/blob/master/images/switch_models.png?raw=true" style="padding-left:15px; padding-bottom:20px" title="Tooling for dictionary-driven code generation" align="right" width="299px"/>

## Introduction

Bitemp Remodeler for SQL Developer is a code generator for Oracle SQL Developer. It generates code to switch between non-temporal, uni-temporal and bi-temporal models while preserving data. The generated table API provides compatibility for existing applications, handles temporal DML and supports temporal queries. 

Business logic may be placed in hooks, to be called before or after an insert, update or delete. These hooks are implemented in an optional PL/SQL package body. Optional means that the generated code runs without the hook package body.

For efficient bulk operations, dedicated procedures for initial and delta load operations are generated.

## Modeling Principles

### Use Latest Database Features

The Oracle Database 12c supports the transaction time dimension through flashback data archive and the valid time dimension through temporal validity. Bitemp Remodeler uses these features to support the following four models:

<img src="https://github.com/oddgen/bitemp/blob/master/images/four_models.png?raw=true" title="Four models"/>

### Switching To Any Model

To keep the model simple you use non-temporal tables. However, if you need to switch to a temporal model, it should be as easy as possible.

When switching from a source to a target model

* The data is kept (if the target model may store it)
* The table API stays compatible for the latest table, so no application changes are necessary 
* The table API for history table and history functionality is target model specific and requires application changes

### Latest Table

The latest table is maintained for all model variants. Each row represents the latest (newest, youngest) period. This period is not the actual valid row when periods starting in the future are managed.

The latest table contains all columns of the original non-temporal table plus

| Column            | Comment       | Override Name in Generator? |
| :---------------- | :------------ | :-------------------------: |
|```IS_DELETED$``` | This column is added for uni-temporal valid time and bi-temporal models. ```1``` means that a period is marked as deleted, ```NULL``` means that the period is active (default) | No |
### History Table

The history table is maintained for uni-temporal valid time and bi-temporal models. Each row represents a period.
The history table contains all columns of the latest table plus

| Column            | Comment       | Override Name in Generator? |
| :---------------- | :------------ | :-------------------------: |
|```HIST_ID$```    | primary key, identity column, always generated | No |
|```VT_START```    | start of valid time period | Yes |
|```VT_END```      | end of valid time period | Yes ||```VT$```	        | hidden virtual column, temporal validity period definition | No |
### Temporal vs. Non-Temporal Columns

All columns in a temporal table are temporal. This sounds obvious, but it is not. Usually you define temporality per column and not per table. For example in a dimensional data mart model you assign the slowly changing dimension type SCD1 or SCD2 per column. This definition drives the creation of a new dimension record. It is basically the information why a table is temporal. To not loose this information, it is recommended to amend the table and/or column comments accordingly. 

Technically this simplifies the model definition, since there is no formal need to distinguish between temporal and non-temporal columns.

But the drawback is, that a change on non-temporal columns may create additional - from a business perspective - unnecessary periods.
### Periods and Temporal Constraints

Oracle uses right-open intervals for periods in flashback data archive and temporal validity. This means that ```VT_START``` is part of the period but ```VT_END``` is not. To query the actual valid data, you have to define a filter condition as the following one:

```
WHERE (vt_start IS NULL OR vt_start <= SYSTIMESTAMP)
  AND (vt_end IS NULL OR vt_end > SYSTIMESTAMP) 
```

The following temporal constraints are enforced for uni-temporal valid time and bi-temporal models:

* No gaps between periods   * First ```VT_START``` ```IS NULL```   * Last ```VT_END``` ```IS NULL```   * Deleted periods are identified with the condition ```IS_DELETED$ = 1```
   * No overlapping periods* No invalid periods* No duplicate periods* Merge identical, connected periods immediately

### Original Primary Key

* Identification over all periods* Foreign key constraint on history table to latest table* Unique constraint for original primary key columns plus ```VT_START``` in history table* Must not be changed (use surrogate key, if this is not acceptable)

### Original Foreign Keys

* No foreign key constraints on history table for original foreign keys* Non-unique indexes on original foreign keys in history table

### Temporal Example Data Model

The diagram looks the same for a uni-temporal valid time and bi-temporal data model. In a bi-temporal model a flashback data archive is associated with the history tables ```EMP_HT``` and ```DEPT_HT```.

<img src="https://github.com/oddgen/bitemp/blob/master/images/temporal_model.png?raw=true" title="Temporal Example Model"/>

## Temporal DML

### Generated Table API

For the temporal example model above, the following objects are generated as part of the table API:

| Object Type | Object Name | Description |
| ----------- | ----------- | ----------- | 
| View | ```EMP(_LV)``` | Latest view, latest rows only, updateable |
| | ```EMP_HV``` | History view, all rows, updateable |
| | ```EMP_FHV``` | Full history view, all rows, FBA version columns, read-only |
| Trigger | ```EMP_TRG``` | Instead-of-trigger on ```EMP(_LV)```, calls ```EMP_API.ins```, ```EMP_API.upd``` and ```EMP_API.del``` |
| | ```EMP_HV_TRG``` | Instead-of-trigger on ```EMP_HV```, calls ```EMP_API.ins```, ```EMP_API.upd``` and ```EMP_API.del```. |
| Package | ```EMP_API``` | API package specification with procedures  ```ins```, ```upd```, ```del```, ```init_load```, ```delta_load```, ```create_load_tables``` and ```set_debug_output``` |
| | ```EMP_HOOK``` | Package specification with procedures ```pre_ins```, ```post_ins```, ```pre_upd```, ```post_upd```, ```pre_del``` and ```post_del```. No package body is generated. The implementation of the body is optional. In fact the API ignores errors caused by a missing hook package body. |
| Package Body | ```EMP_API``` | API package body with implementation of the public procedures ```ins```, ```upd```, ```del```, ```init_load```, ```delta_load```, ```create_load_tables``` and ```set_debug_output``` |
| Type | ```EMP_OT``` | Object type for ```EMP_HT``` columns |
| | ```EMP_CT``` | Collection type, table of emp_ot |
| Type Body | ```EMP_OT``` | Type body with default constructor implementation |

All suffixes may be overridden when running the generator.

From a user point of view the most important objects are the views and the packages. They are the public interface of the table API.

### Temporal INSERT

* Adds a period to a new or existing object   * Deletes enclosing, existing periods   * Adjusts validity of existing, overlapping periods* ```SYSTIMESTAMP``` is used for ```VT_START``` for inserts on latest view* Enforces temporal constraints* Keeps history and latest table in sync

### Temporal UPDATE

* Period changed only (```vt_start```, ```vt_end```)   * Adjust validity of overlapping periods   * Update **all** columns in affected periods   * Requires period to be enlarged to have an impact* Application columns changed (```ename```, ```job```, ```mgr```, ```hiredate```, ```sal```, ```comm```, ```deptno```)   * Adjust validity of overlapping periods   * Update **changed** columns in all affected periods* Enforces temporal constraints* Keeps history and latest table in sync ### Temporal DELETE

* Delete period from an existing object   * Adjust validity of overlapping periods   * Set ```IS_DELETED$``` to ```1``` in affected periods* Enforces temporal constraints* Keeps history and latest table in sync * Deleting a non-existent period is supported via ```EMP_API.DEL``` or ```EMP_API.INS```* Deleted periods are not shown in updateable latest view* Deleted periods are visible in updateable history view and in read-only full history view 

### Bulk Processing

Use the procedures ```create_load_tables```, ```init_load``` and ```delta_load``` for processing large data sets. See documentation in generated package specification for details.
## Releases

Binary releases are published [here](https://github.com/oddgen/bitemp/releases).

## Issues
Please file your bug reports, enhancement requests, questions and other support requests within [Github's issue tracker](https://help.github.com/articles/about-issues/).

* [Questions](https://github.com/oddgen/bitemp/issues?q=is%3Aissue+label%3Aquestion)
* [Open enhancements](https://github.com/oddgen/bitemp/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement)
* [Open bugs](https://github.com/oddgen/bitemp/issues?q=is%3Aopen+is%3Aissue+label%3Abug)
* [Submit new issue](https://github.com/oddgen/bitemp/issues/new)

## How to Contribute

1. Describe your idea by [submitting an issue](https://github.com/oddgen/bitemp/issues/new)
2. [Fork the bitemp repository](https://github.com/oddgen/bitemp/fork)
3. [Create a branch](https://help.github.com/articles/creating-and-deleting-branches-within-your-repository/), commit and publish your changes and enhancements
4. [Create a pull request](https://help.github.com/articles/creating-a-pull-request/)

## How to Build

1. [Download](http://www.oracle.com/technetwork/developer-tools/sql-developer/downloads/index.html) and install SQL Developer 4.1.3
2. [Download](https://maven.apache.org/download.cgi) and install Apache Maven 3.1.9
3. [Download](https://git-scm.com/downloads) and install a git command line client
4. Clone the bitemp repository
5. Open a terminal window in the bitemp root folder and type 

		cd sqldev
		
6. Run maven build by the following command

		mvn -Dsqldev.basedir=/Applications/SQLDeveloper4.1.3.app/Contents/Resources/sqldeveloper -DskipTests=true clean package
		
	Amend the parameter sqldev.basedir to match the path of your SQL Developer installation. This folder is used to reference various Oracle jar files which are not available in public Maven repositories
7. The resulting file ```bitemp_for_SQLDev_x.x.x-SNAPSHOT.zip``` may be installed within SQL Developer

## License

Bitemp Remodeler for SQL Developer is licensed under the Apache License, Version 2.0. You may obtain a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>. 
