# Bitemp Remodeler for SQL Developer

<img src="https://github.com/oddgen/oddgen/blob/master/sqldev/src/main/resources/org/oddgen/sqldev/resources/images/oddgen_512x512_text.png?raw=true" style="padding-left:15px; padding-bottom:20px" title="Tooling for dictionary-driven code generation" align="right" width="128px" />

## Introduction

Bitemp Remodeler for SQL Developer is a code generator for Oracle SQL Developer. It generates code to switch between non-temporal, uni-temporal and bi-temporal models while preserving data. The generated table API provides compatibility for existing applications, handles temporal DML and supports temporal queries. 

Business logik may be placed in pre-/post insert/update/delete hooks. These hooks are implemented in an optional PL/SQL package body. Optional means that the generated code runs without the hook package body.

For efficient bulk operations, dedicated procedures for initial and delta load operations are generated.

## Releases

The initial release will be published the second half of September 2016.

## Issues
Please file your bug reports, enhancement requests, questions and other support requests within [Github's issue tracker](https://help.github.com/articles/about-issues/).

* [Questions](https://github.com/oddgen/bitemp/issues?q=is%3Aissue+label%3Aquestion)
* [Open enhancements](https://github.com/oddgen/bitemp/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement)
* [Open bugs](https://github.com/oddgen/bitemp/issues?q=is%3Aopen+is%3Aissue+label%3Abug)
* [Submit new issue](https://github.com/oddgen/bitemp/issues/new)

## How to Contribute

1. Describe your idea by [submitting an issue](https://github.com/oddgen/bitemp/issues/new)
2. [Fork the bitemp respository](https://github.com/oddgen/bitemp/fork)
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
