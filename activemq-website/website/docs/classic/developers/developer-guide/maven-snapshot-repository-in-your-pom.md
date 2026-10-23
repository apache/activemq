---
title: "Maven SNAPSHOT Repository in your POM"
slug: /maven-snapshot-repository-in-your-pom
---


ActiveMQ

In your pom.xml file you can add the Maven 2 snapshot repository if you want to try out the SNAPSHOT versions:
```
<repository>
  <id>apache.snapshots</id>
  <name>Apache Development Snapshot Repository</name>
  <url>https://repository.apache.org/content/repositories/snapshots/</url>
  <releases>
    <enabled>false</enabled>
  </releases>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
</repository>
```
Then you can use the SNAPSHOT version of ActiveMQ in your `pom.xml`:
```
<dependency>
  <groupId>org.apache.activemq</groupId>
  <artifactId>activemq-broker</artifactId>
  <version>5.14.0-SNAPSHOT</version>
</dependency>
```
Apache ActiveMQ SNAPSHOTS

[https://repository.apache.org/content/repositories/snapshots/](https://repository.apache.org/content/repositories/snapshots/)

