---
title: "Maven2 ActiveMQ Broker Plugin"
slug: /maven2-activemq-broker-plugin
---


ActiveMQ provides a Maven2 plugin to easily startup a JMS broker. It is useful to quickly boot up a message broker in your Maven2 project for debugging or for doing integration tests.

###  How to Use

> Be Careful
> 
> The maven plugins in ActiveMQ have been renamed in version 5.8.0+ to better follow the Maven plugin naming conventions. The ActiveMQ Broker plugin has changed from 'maven-activemq-plugin' to 'activemq-maven-plugin'.

> Replace the 5.7.0 version string with the version of your choosing, eg: 5.6.0, 5.8-SNAPSHOT

In your Maven2 project, or using a minimal pom.xml like the following:
```
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.acme</groupId>
  <artifactId>run-amq</artifactId>
  <version>1.0-SNAPSHOT</version>
</project>
```
just type:
```
mvn org.apache.activemq.tooling:maven-activemq-plugin:5.1:run
```
This will download the 5.7.0 version of the plugin, and spin up a broker using a simple configuration url of the form `broker:(tcp://localhost:61616)?useJmx=false&persistent=false`. The necessary ActiveMQ jars will automatically be downloaded by Maven.

To configure log4j, provide the `-Dlog4j.configuration=[file:///](file:///)<full path to log4j.properties>` system property on the mvn command line.

If you require a more advanced configuration with spring support, the jetty webconsole or with embedded camel you can add the plugin in your pom.xml and provide the required optional dependencies. For the default `conf/activemq.xml`, the following dependencies are required :
```
 <build>    
    <plugins>
      <plugin>
        <groupId>org.apache.activemq.tooling</groupId>
        <artifactId>maven-activemq-plugin</artifactId>
        <version>5.7.0</version>
        <configuration>
          <configUri>xbean:file:../conf/activemq.xml</configUri>
          <fork>false</fork>
          <systemProperties>
            <property>
              <name>javax.net.ssl.keyStorePassword</name>
              <value>password</value>
            </property>
            <property>
              <name>org.apache.activemq.default.directory.prefix</name>
              <value>./target/</value>
            </property>
          </systemProperties>
        </configuration>
        <dependencies>
			  <dependency>
		      <groupId>org.springframework</groupId>
			    <artifactId>spring</artifactId>
		      <version>2.5.5</version>
			  </dependency>
			  <dependency>
		      <groupId>org.mortbay.jetty</groupId>
		      <artifactId>jetty-xbean</artifactId>
		      <version>6.1.11</version>
		    </dependency> 	
		    <dependency>
		      <groupId>org.apache.camel</groupId>
		      <artifactId>camel-activemq</artifactId>
		      <version>1.1.0</version>
        </dependency>
      </dependencies>			
    </plugin>
  </plugins>
</build>
``
 and run it using:
```
mvn activemq:run
```

###  Configuration Options

Name|Default|Description
---|---|---
configUri|broker:(tcp://localhost:61616)?useJmx=false&persistent=false|The broker configuration URI that will be use to startup the broker. For more information, refer [here](../overview/faq/using-apache-activemq-classic/how-do-i-embed-a-broker-inside-a-connection)  
fork|false|If true, start up the broker in a separate thread, enabling maven to continue processing (Useful for integration testing).  
systemProperties|none|Additional system properties that will be set.  

**Note**: By default, the broker plugin will set activemq.base, activemq.home, org.apache.activemq.default.directory.prefix, derby.system.home to ./target/. This means that all data folders will be created there, hence will easily be deleted by running mvn clean.

