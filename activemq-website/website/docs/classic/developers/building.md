---
title: "Building"
slug: /building
---


Building ActiveMQ from Source
-----------------------------

ActiveMQ uses [Maven](http://maven.apache.org/) as its build and management tool. If you don't fancy using Maven you can use your IDE directly or [Download](/components/classic/download) a distribution or JAR.

### Prequisites

**Required:**

*   Download and [install Maven](http://maven.apache.org/download).
*   Get the latest [Source](source)
*   JDK (11 for version 5.17.0+, 1.8 for version <= 5.17.0, 1.6 for version <= 5.10, 1.7 for version > 5.10)

Using Maven 3
-------------------------------------

ActiveMQ uses Maven 3 to Build. We recommend you download and install [Maven](http://maven.apache.org/download).

> You should set the MAVEN_OPTS environment variable to at least -Xmx800m. There are portions of the ActiveMQ build that are very memory intensive. Increase the maven memory limit so that the build does not fail for you.

### Doing a Quick Build
```
mvn -DskipTests -DfailIfNoTests=false clean install 
```
### Using an IDE

If you prefer to use an IDE then you can auto-generate the IDE's project files using maven plugins. e.g.
```
mvn eclipse:eclipse
```
or
```
mvn idea:idea
```

> **Importing into Eclipse**
> 
> If you have not already done so, you will need to make Eclipse aware of the Maven repository so that it can build everything. In the preferences, go to Java->Build Path->Classpath and define a new Classpath Variable named M2_REPO that points to your local Maven repository (i.e., `~/.m2/repository` on Unix and `c:\Documents and Settings\<user>\.m2\repository` on Windows).

### Other Maven 2 Goals

For more details try the [Examples](examples) or [Benchmark Tests](benchmark-tests)  
Please refer to the [plugin reference](http://maven.apache.org/plugins/index.html) for more details on using them.

Using Maven 1 (ActiveMQ 4.0.x and Down)
---------------------------------------

ActiveMQ 4.0.x and down use Maven 1 to Build. We recommend you download and install [Maven 1.0.2](http://maven.apache.org/maven-1.x/startOverviewOverview/Overview/download).

### Doing a Quick Build
```
maven -Dmaven.test.skip.exec=true
```
### Using an IDE

If you prefer to use an IDE then you can autogenerate the IDE's project files using maven plugins. e.g.
```
maven eclipse
```
or
```
maven idea
```
etc.

### Other Maven 1 Goals

For more details try the [Examples](examples) or [Benchmark Tests](benchmark-tests)  
Please refer to the [plugin reference](http://maven.apache.org/maven-1.x/plugins/bundled/) for more details on using them.

