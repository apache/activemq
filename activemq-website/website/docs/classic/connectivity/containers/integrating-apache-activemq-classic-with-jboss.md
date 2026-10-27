---
title: "Integrating Apache ActiveMQ with JBoss"
slug: /integrating-apache-activemq-classic-with-jboss
---


Integrating Apache ActiveMQ with JBoss
======================================

Integration with application servers is a common scenario in the enterprise Java world, especially when it comes to messaging. ActiveMQ is a JMS 1.1 compliant, open source, Apache Licensed, message oriented middleware (MOM) with [many](changes-in-40), [many](new-features-in-41) features far beyond the JMS specification. ActiveMQ offers many different [points of connectivity](..), many [cross language clients](cross-language-clients) and many [pluggable transport protocols](protocols) including integration with any J2EE 1.4 application server.

One of the application servers in the open source world is JBoss. A very common requirement is to configure ActiveMQ as the messaging infrastructure within JBoss. Although there is [a bit of documentation](jboss-integration) on this integration, this article seeks to provide much more detail and explanation. So if you have a need to integrate ActiveMQ with JBoss, this article is for you.

This article explains how to configure JBoss to start up ActiveMQ as part of its lifecycle and how to configure the ActiveMQ resource adapter to handle the messaging and transactionality between ActiveMQ and JBoss.

Requirements
------------

Below are the software requirements for this article with links to download each:

*   [Apache ActiveMQ 4.0.1+](/components/classic/download)
*   [JBoss 4.0.4+](http://sourceforge.net/project/showfiles.php?group_id=22866&package_id=16942&release_id=416591)
*   [Sun Java 1.5+](http://www.java.com/en/download/index.jsp)
*   [Apache Ant 1.6+](http://ant.apache.org/bindownload.cgi)

Though this article is using Unix, the installation and integration will work on any platform running Sun Java. It is recommended that each piece of software be downloaded before working through the steps is this article. Once each piece of software has been downloaded, proceed to the first step.

Install the J2SE 1.5
--------------------

The first step in this process is to install Java 1.5 and verify that it runs correctly. Using the link above, find, download and install the correct version of Java for your platform. Once Java is installed and in the PATH, test it to see that it runs correctly using the following command:
```
$ java -version
java version "1.5.0_06"
Java(TM) 2 Runtime Environment, Standard Edition (build 1.5.0_06-112)
Java HotSpot(TM) Client VM (build 1.5.0_06-64, mixed mode, sharing)
```
Depending upon your platform and the exact build number of Java 1.5, your output may vary slightly. As long as it's Java 1.5 and the version information is output, you should be ready to proceed to the next step.

Install Apache Ant
------------------

The second step is to install Apache Ant. Simply download it using the link above, expand somewhere on your hard drive and place the `bin` directory in the PATH. This will allow you to test it quickly using the commands below:
```
$ ant -version
Apache Ant version 1.6.2 compiled on July 16 2004
```
As long as you see the version output above, Ant should be usable. If you did not see the version output or received an error, consult the Ant documentation or mailing list archives to work out the issue. Please note that Ant is not used until the end of this whole exercise to test the integration.

Install the JBoss Application Server
------------------------------------

The third step in this process is to install JBoss and make sure it runs correctly before installing and configuring ActiveMQ. Upon downloading JBoss-4.0.4, expand it in a place where it can create a directory. Next, run the server using the following commands:
```
$ cd jboss-4.0.4.GA
$ ./bin/run.sh -c default
=========================================================================

  JBoss Bootstrap Environment

  JBOSS_HOME: /opt/jboss-4.0.4.GA

  JAVA: java

  JAVA_OPTS: -server -Xms128m -Xmx512m -Dsun.rmi.dgc.client.gcInterval=3600000 
-Dsun.rmi.dgc.server.gcInterval=3600000 -Dprogram.name=run.sh

  CLASSPATH: /opt/jboss-4.0.4.GA/bin/run.jar:/lib/tools.jar

=========================================================================

15:34:47,999 INFO  [Server] Starting JBoss (MX MicroKernel)...
15:34:48,001 INFO  [Server] Release ID: JBoss [Zion] 4.0.4.GA (build: CVSTag=JBoss\_4\_0\_4\_GA date=200605151000)
15:34:48,004 INFO  [Server] Home Dir: /opt/jboss-4.0.4.GA
15:34:48,005 INFO  [Server] Home URL: file:/opt/jboss-4.0.4.GA/
15:34:48,007 INFO  [Server] Patch URL: null
15:34:48,007 INFO  [Server] Server Name: default
15:34:48,007 INFO  [Server] Server Home Dir: /opt/jboss-4.0.4.GA/server/default
15:34:48,011 INFO  [Server] Server Home URL: file:/opt/jboss-4.0.4.GA/server/default/
15:34:48,012 INFO  [Server] Server Log Dir: /opt/jboss-4.0.4.GA/server/default/log
15:34:48,017 INFO  [Server] Server Temp Dir: /opt/jboss-4.0.4.GA/server/default/tmp
15:34:48,022 INFO  [Server] Root Deployment Filename: jboss-service.xml
...
15:35:17,360 INFO  [Server] JBoss (MX MicroKernel) [4.0.4.GA (build: CVSTag=JBoss\_4\_0\_4\_GA date=200605151000)] 
Started in 22s:238ms
```
The first few lines of output from the JBoss startup indicates the configuration being used but the last line is the most important one. It tells you that JBoss has been started successfully on your system. For an extra measure of assurance, visit [http://localhost:8080/web-console/](http://localhost:8080/web-console/) in a web browser to make sure you are able to see the JBoss web console. If you can see this console, everything should be ready to go.

As a side note, the left-hand side of the web-console requires that the [Java plugin](http://java.sun.com/products/plugin/) be properly installed. This is supposed to take place when installing the J2SE, so if it did not work correctly for you, I suggest that you consult some documentation about any errors you may be seeing. The Java plugin is not required for JBoss or ActiveMQ to function properly, it is simply for the JBoss web-console.

Once JBoss is installed properly, shut it down using either the shutdown script or by simply typing ctrl-c to activate the shutdown hook. Once it is shut down, proceed to the next step.

Prepare for Integrating Apache ActiveMQ with the JBoss Application Server
-------------------------------------------------------------------------

The fourth step is to prepare your environment for integrating ActiveMQ with JBoss. If you haven't done so already, download Apache ActiveMQ using the link above. As of the writing of this article, the latest released version is 4.0.2-RC4. Upon downloading this archive, expand it in a place where it can create a directory, preferably in the same location where JBoss was expanded. Verify that the ActiveMQ RAR file is included using the following commands:
```
$ cd ./incubator-activemq-4.0.2.tar.gz
$ jar tvf /opt/incubator-activemq-4.0.2/lib/optional/activemq-ra-4.0.2.rar
     0 Mon Sep 25 19:00:50 MDT 2006 META-INF/
   399 Mon Sep 25 19:00:48 MDT 2006 META-INF/MANIFEST.MF
   523 Mon Sep 25 13:07:40 MDT 2006 META-INF/DISCLAIMER.txt
 11359 Mon Sep 25 13:07:40 MDT 2006 META-INF/LICENSE.txt
 20305 Mon Sep 25 13:07:40 MDT 2006 META-INF/ra.xml
  2334 Mon Sep 25 13:07:40 MDT 2006 broker-config.xml
 70023 Mon Sep 25 19:00:48 MDT 2006 activemq-ra-4.0.2.jar
1351744 Mon Sep 25 19:00:14 MDT 2006 activemq-core-4.0.2.jar
232107 Mon Sep 25 18:22:48 MDT 2006 activeio-core-3.0-beta4.jar
2143211 Tue Aug 01 11:44:50 MDT 2006 derby-10.1.1.0.jar
1901653 Tue Aug 01 11:47:08 MDT 2006 spring-1.2.6.jar
 94713 Tue Aug 01 11:47:22 MDT 2006 xbean-spring-2.2.jar
 52915 Tue Aug 01 12:22:16 MDT 2006 commons-logging-1.1.jar
 16030 Tue Aug 01 12:22:32 MDT 2006 geronimo-j2ee-management\_1.0\_spec-1.0.jar
329586 Tue Aug 01 12:22:54 MDT 2006 backport-util-concurrent-2.1.jar
358085 Tue Aug 01 12:23:26 MDT 2006 log4j-1.2.12.jar
   523 Mon Sep 25 19:00:50 MDT 2006 META-INF/DISCLAIMER.txt
 11359 Mon Sep 25 19:00:50 MDT 2006 META-INF/LICENSE.txt
```
This is simply a table of contents of the RAR file. There should only be one reason that this will fail - an incomplete download of the ActiveMQ tarball or zip file. Beyond that, depending on the version you download, some of the library versions may be slightly different.

Now go back to the JBoss installation and create a directory for ActiveMQ in the JBoss deploy directory for the default JBoss context. Below are the commands to achieve this task:
```
$ mkdir /opt/jboss-4.0.4.GA/server/default/deploy/activemq-ra.rar
$ cd /opt/jboss-4.0.4.GA/server/default/deploy/activemq-ra.rar
$ pwd
/opt/jboss-4.0.4.GA/server/default/deploy/activemq-ra.rar
```
`_NOTE:_** The creation of a directory is not required but is the easiest way to set up the ActiveMQ RAR when you're just getting started. This is due to the flexibility it affords during the development phase for the configuration to be changed very easily. The alternative is to JAR up the directory as a RAR file once the configuration is solid enough that it no longer needs to be changed. But leaving everything in a directory during development is the easiest path.

Now expand the activemq-ra-4.0.2.rar into the current working directory:
```
jar xvf /opt/incubator-activemq-4.0.2/lib/optional/activemq-ra-4.0.2.rar
  created: META-INF/
 inflated: META-INF/MANIFEST.MF
 inflated: META-INF/DISCLAIMER.txt
 inflated: META-INF/LICENSE.txt
 inflated: META-INF/ra.xml
 inflated: broker-config.xml
 inflated: activemq-ra-4.0.2.jar
 inflated: activemq-core-4.0.2.jar
 inflated: activeio-core-3.0-beta4.jar
 inflated: derby-10.1.1.0.jar
 inflated: spring-1.2.6.jar
 inflated: xbean-spring-2.2.jar
 inflated: commons-logging-1.1.jar
 inflated: geronimo-j2ee-management\_1.0\_spec-1.0.jar
 inflated: backport-util-concurrent-2.1.jar
 inflated: log4j-1.2.12.jar
 inflated: META-INF/DISCLAIMER.txt
 inflated: META-INF/LICENSE.txt
```
Below is a quick listing of the contents of that directory:
```
$ ls -l
total 12848
drwxr-xr-x   6 bsnyder  bsnyder      204 Oct 16 16:27 META-INF
-rw-r--r--   1 bsnyder  bsnyder   232107 Oct 16 16:27 activeio-core-3.0-beta4.jar
-rw-r--r--   1 bsnyder  bsnyder  1351744 Oct 16 16:27 activemq-core-4.0.2.jar
-rw-r--r--   1 bsnyder  bsnyder    70023 Oct 16 16:27 activemq-ra-4.0.2.jar
-rw-r--r--   1 bsnyder  bsnyder   329586 Oct 16 16:27 backport-util-concurrent-2.1.jar
-rw-r--r--   1 bsnyder  bsnyder     2334 Oct 16 16:27 broker-config.xml
-rw-r--r--   1 bsnyder  bsnyder    52915 Oct 16 16:27 commons-logging-1.1.jar
-rw-r--r--   1 bsnyder  bsnyder  2143211 Oct 16 16:27 derby-10.1.1.0.jar
-rw-r--r--   1 bsnyder  bsnyder    16030 Oct 16 16:27 geronimo-j2ee-management\_1.0\_spec-1.0.jar
-rw-r--r--   1 bsnyder  bsnyder   358085 Oct 16 16:27 log4j-1.2.12.jar
-rw-r--r--   1 bsnyder  bsnyder  1901653 Oct 16 16:27 spring-1.2.6.jar
-rw-r--r--   1 bsnyder  bsnyder    94713 Oct 16 16:27 xbean-spring-2.2.jar
```
Now it's time to configure ActiveMQ.

Configuring Apache ActiveMQ
---------------------------

The fifth step is to actually configure ActiveMQ for integration with JBoss. Remember that you should be sitting in the following directory:
```
/opt/jboss-4.0.4.GA/server/default/deploy/activemq-ra.rar
```
You may or may not have installed JBoss in `/opt`, that doesn't particularly matter. What does matter is that you're sitting in the directory that was created above to hold the contents of the expanded ActiveMQ RAR file.

`_NOTE:_** A RAR file is a Resource adapter ARchive (RAR). Resource adapters are a concept from the [J2EE Connector Architecture (JCA)](http://java.sun.com/j2ee/connector/) and are used to interface with Enterprise Information Systems (EIS), i.e., systems external to the application server (e.g., relational databases, mainframes, MOMs, accounting systems, etc.). Resource adapters are often referred to as J2EE connectors and are very similar to the concept of a device driver for, say, a printer in that they contain information specific to connecting to a particular system. The difference with JCA is that that connection has been formalized in specification for Java. So the overall concepts of JCA is for connection to any EIS, but what does that mean? JCA 1.5 provides connectivity and more via the following contracts:

Version 1.0 Contracts

In version 1.0 of the Connector Architecture, three contracts are defined to address the functions mentioned above:

*   Connection Management Contract: Lets applications connect to an EIS through the resource adapter. It also allows the application server to pool connection requests to the EIS.
*   Transaction Management Contract: Allows an application to manage and perform transactional access across one-to-many EIS resource managers.
*   Security Contract: Provides support for secure access to the EIS.

New Contracts in Version 1.5

In version 1.5 of the J2EE Connector Architecture, there are more contracts that a resource adapter must support, as new functionality and features made their way into the specification. A resource adapter can support these four new contracts by implementing the required interfaces defined in the specification for each contract.

*   Lifecycle Management Contract: Lets the application server manage the lifecycle – that is, the startup and shutdown functionality – of the resource adapter.
*   Work Management Contract: Allows the resource adapter to do work by submitting it to an application server for execution. Since the application server does the work for the resource adapter, the resource adapter needn't worry about thread management. Instead, the application server manages this aspect efficiently and can use thread pooling if necessary. Although the work management contract is not required (the resource adapter can choose to manage its own thread for work), it is definitely recommended.
*   Transaction Inflow Contract: Allows a resource adapter to propagate an imported transaction to an application server, as well as flow-in transaction completion and crash recovery initiated by an EIS.
*   Message Inflow Contract: Allows the resource adapter to synchronously or asynchronously deliver messages to endpoints in the application server, irrespective of message style, semantics, and infrastructure.

Quoted from [What's New in the J2EE Connector Architecture 1.5](http://java.sun.com/developer/technicalArticles/J2EE/connectorarch1_5/)

For more information about JCA, please consult the [J2EE Connector Architecture documentation](http://java.sun.com/j2ee/connector/index.jsp).

Open the `META-INF/ra.xml` file and look for the following section:

`META-INF/ra.xml**
```xml
<config-property>
    <description>
      The URL to the ActiveMQ server that you want this connection to connect to.  If using
      an embedded broker, this value should be 'vm://localhost'.
    </description>
    <config-property-name>ServerUrl</config-property-name>
    <config-property-type>java.lang.String</config-property-type>
    <config-property-value>tcp://localhost:61616</config-property-value>
<!-- <config-property-value>vm://localhost</config-property-value> -->
  </config-property>
```
The section above is used to tell the ActiveMQ RAR where ActiveMQ is located. By default, the in-VM protocol is commented out in favor of the tcp protocol. This will find ActiveMQ running on any interface on the localhost on port 61616. It's ok to just leave this alone if you don't mind the inefficiency of communicating within the JVM via TCP. However, it is recommended that `vm://` transport be used for an embedded broker, so comment out the `tcp://` transport and uncomment the `vm://` transport. Below is an example of this:

`META-INF/ra.xml**
```xml
<config-property>
    <description>
      The URL to the ActiveMQ server that you want this connection to connect to.  If using
      an embedded broker, this value should be 'vm://localhost'.
    </description>
    <config-property-name>ServerUrl</config-property-name>
    <config-property-type>java.lang.String</config-property-type>
    <!-- <config-property-value>tcp://localhost:61616</config-property-value> -->
    <config-property-value>vm://localhost</config-property-value>
  </config-property>
```
Because we're embedding ActiveMQ inside of JBoss, it is more efficient to use the `vm://` transport, rather than to perform messaging over the `tcp://` transport.

Now look further down the `META-INF/ra.xml` file and locate the following section:

`META-INF/ra.xml**
```xml
<config-property>
    <description>
      Sets the XML configuration file used to configure the embedded ActiveMQ broker via
      Spring if using embedded mode.

      BrokerXmlConfig is the filename which is assumed to be on the classpath unless
      a URL is specified. So a value of foo/bar.xml would be assumed to be on the
      classpath whereas file:dir/file.xml would use the file system.
      Any valid URL string is supported.
    </description>
    <config-property-name>BrokerXmlConfig</config-property-name>
    <config-property-type>java.lang.String</config-property-type>
    <config-property-value></config-property-value>
    <!--
    <config-property-value>xbean:broker-config.xml</config-property-value>
    -->
  </config-property>
```
The section above needs to be changed to uncomment the second to last line and remove/replace the empty element that is above it. Below is an example of how this should be changed:

`META-INF/ra.xml**
```xml
<config-property>
    <description>
      Sets the XML configuration file used to configure the embedded ActiveMQ broker via
      Spring if using embedded mode.

      BrokerXmlConfig is the filename which is assumed to be on the classpath unless
      a URL is specified. So a value of foo/bar.xml would be assumed to be on the
      classpath whereas file:dir/file.xml would use the file system.
      Any valid URL string is supported.
    </description>
    <config-property-name>BrokerXmlConfig</config-property-name>
    <config-property-type>java.lang.String</config-property-type>
    <config-property-value>xbean:broker-config.xml</config-property-value>
  </config-property>
```
This change tells the ActiveMQ RAR to read a configuration file named `broker-config.xml` (the `xbean:` that proceeds the filename is simply a hint to class doing the reading of the configuration file) which is located on the CLASSPATH. In this case, the `broker-config.xml` file is located in the `activemq-ra.rar` directory. Save the changes to that file and then open the `broker-config.xml` file.

The `broker-config.xml` file _is_ the ActiveMQ configuration file. This is the file used to configure ActiveMQ. The default contents of this file are usable, but should be customized to suit your environment. There are several items of note about this configuration. The most prominent sections to note in this file are the `<persistenceAdapter>` element and the `<transportConnectors>` and `<networkConnectors>` elements as seen below:

`broker-config.xml**
```xml
<beans xmlns="http://activemq.org/config/1.0">

  <broker useJmx="true">
  ...
    <persistenceAdapter>
      <journaledJDBC journalLogFiles="5" dataDirectory="activemq-data"/>
      <!-- To use a different datasource, use th following syntax : -->
      <!--
      <journaledJDBC journalLogFiles="5" dataDirectory="../data" dataSource="#postgres-ds"/>
       -->
    </persistenceAdapter>

    <transportConnectors>
      <!-- prefixing a connector with discovery: causes the connector to be advertiesed over rendezvous -->
      <transportConnector uri="tcp://localhost:61616" discoveryUri="multicast://default"/>
    </transportConnectors>

    <networkConnectors>
      <!-- by default just auto discover the other brokers -->
      <networkConnector uri="multicast://default"/>
      <!--
      <networkConnector uri="static://(tcp://host1:61616,tcp://host2:61616)"/>
      -->
    </networkConnectors>

  </broker>

</beans>
```
The first change to this file is to add the brokerName attribute to the broker element and provide a name:
```xml
<broker useJmx="true" brokerName="bruce.broker1">
```
In addition, this same name is used further down the configuration to provide a name for the `<transportConnector>` element:
```xml
<transportConnector name="bruce.broker1" uri="tcp://localhost:61616" discoveryUri="multicast://default"/>
```
Now we'll tell ActiveMQ not to initialize JMX because we'll use the existing one that JBoss has:
```xml
<!-- put the following as the first child of the broker tag -->
<managementContext><!-- we'll use an existing one (JBoss) instead of creating one -->
  <managementContext createConnector="false"/>
</managementContext>
```
The `<persistenceAdapter>` element should be reconfigured to store its data in an appropriate place. On JBoss, that's most likely within the "data" directory of the server configuration you're using. We're going to set this dynamically using an XBean and Spring feature that allows us to inject system properties values into the configuration. First this needs to be enabled:
```xml
<!-- put the following as the first child of the beans element -->
<bean xmlns="" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>
```
Now, modify the `dataDirectory` attribute of the `journaledJDBC` element to be the following: ``${jboss.server.data.dir}`/activemq`.

The `<transportConnectors>` element is used to advertise the ActiveMQ broker for client-to-broker communications and the `<networkConnectors>` element advertises the ActiveMQ broker for broker-to-broker communications. The default configuration is to use the ActiveMQ [multicast transport](multicast-transport-reference) for both. This is simply an easy configuration under which to get ActiveMQ up and running, so we'll just leave it at that for the time being.

`_NOTE:_** There are far more configuration options available for ActiveMQ than are noted here. The configuration above is only enough to just get ActiveMQ up and running, nothing more. For more information on the ActiveMQ configuration, see the [ActiveMQ 4.1 XML Reference](xbean-xml-reference-41).

Now we just need to start up JBoss to assure that it comes up correctly without error using the same commands we used previously to start JBoss:
```
$ cd jboss-4.0.4.GA
$ ./bin/run.sh -c default
=========================================================================

  JBoss Bootstrap Environment

  JBOSS_HOME: /opt/jboss-4.0.4.GA

  JAVA: java

  JAVA_OPTS: -server -Xms128m -Xmx512m -Dsun.rmi.dgc.client.gcInterval=3600000 
-Dsun.rmi.dgc.server.gcInterval=3600000 -Dprogram.name=run.sh

  CLASSPATH: /opt/jboss-4.0.4.GA/bin/run.jar:/lib/tools.jar

=========================================================================

15:34:47,999 INFO  [Server] Starting JBoss (MX MicroKernel)...
15:34:48,001 INFO  [Server] Release ID: JBoss [Zion] 4.0.4.GA (build: CVSTag=JBoss\_4\_0\_4\_GA date=200605151000)
15:34:48,004 INFO  [Server] Home Dir: /opt/jboss-4.0.4.GA
15:34:48,005 INFO  [Server] Home URL: file:/opt/jboss-4.0.4.GA/
15:34:48,007 INFO  [Server] Patch URL: null
15:34:48,007 INFO  [Server] Server Name: default
15:34:48,007 INFO  [Server] Server Home Dir: /opt/jboss-4.0.4.GA/server/default
15:34:48,011 INFO  [Server] Server Home URL: file:/opt/jboss-4.0.4.GA/server/default/
15:34:48,012 INFO  [Server] Server Log Dir: /opt/jboss-4.0.4.GA/server/default/log
15:34:48,017 INFO  [Server] Server Temp Dir: /opt/jboss-4.0.4.GA/server/default/tmp
15:34:48,022 INFO  [Server] Root Deployment Filename: jboss-service.xml
...
15:35:17,360 INFO  [Server] JBoss (MX MicroKernel) [4.0.4.GA (build: CVSTag=JBoss\_4\_0\_4\_GA date=200605151000)] 
Started in 28s:576ms
```
As long as JBoss comes up without error, you're ready to move on to the next step.

Configuring JBoss
-----------------

The sixth step is to configure JBoss to initialize and start ActiveMQ whenever JBoss starts up. This is accomplished using an XML file that abides by the [JBoss JCA DTD](http://www.jboss.org/j2ee/dtd/jboss-ds_1_5.dtd) for data sources. Like most other Java application servers on the market, the JBoss architecture uses the J2EE Connector Architecture to manage connections of any kind including JDBC, JMS, etc. and the JBoss JCA DTD denotes the allowed contents for creating an XML data source instance to configure JBoss JCA. Below is an example XML data source instance for use with JBoss:

`activemq-jms-ds.xml**
```xml
<?xml version="1.0" encoding="UTF-8"?>

<!DOCTYPE connection-factories
    PUBLIC "-//JBoss//DTD JBOSS JCA Config 1.5//EN"
    "http://www.jboss.org/j2ee/dtd/jboss-ds\_1\_5.dtd">

<connection-factories>

   <tx-connection-factory>
      <jndi-name>activemq/QueueConnectionFactory</jndi-name>
      <xa-transaction/>
      <track-connection-by-tx/>
      <rar-name>activemq-ra.rar</rar-name>
      <connection-definition>javax.jms.QueueConnectionFactory</connection-definition>
      <ServerUrl>vm://localhost</ServerUrl>
      <!--
      <UserName>sa</UserName>
      <Password></Password>
      -->
      <min-pool-size>1</min-pool-size>
      <max-pool-size>200</max-pool-size>
      <blocking-timeout-millis>30000</blocking-timeout-millis>
      <idle-timeout-minutes>3</idle-timeout-minutes>
   </tx-connection-factory>

   <tx-connection-factory>
      <jndi-name>activemq/TopicConnectionFactory</jndi-name>
      <xa-transaction/>
      <track-connection-by-tx/>
      <rar-name>activemq-ra.rar</rar-name>
      <connection-definition>javax.jms.TopicConnectionFactory</connection-definition>
      <ServerUrl>vm://localhost</ServerUrl>
      <!--
      <UserName>sa</UserName>
      <Password></Password>
      -->
      <min-pool-size>1</min-pool-size>
      <max-pool-size>200</max-pool-size>
      <blocking-timeout-millis>30000</blocking-timeout-millis>
      <idle-timeout-minutes>3</idle-timeout-minutes>
   </tx-connection-factory>

   <mbean code="org.jboss.resource.deployment.AdminObject" name="activemq.queue:name=outboundQueue">
      <attribute name="JNDIName">activemq/queue/outbound</attribute>
      <depends optional-attribute-name="RARName">jboss.jca:service=RARDeployment,name='activemq-ra.rar'</depends>
      <attribute name="Type">javax.jms.Queue</attribute>
      <attribute name="Properties">PhysicalName=queue.outbound</attribute>
   </mbean>

   <mbean code="org.jboss.resource.deployment.AdminObject" name="activemq.topic:name=inboundTopic">
      <attribute name="JNDIName">activemq/topic/inbound</attribute>
      <depends optional-attribute-name="RARName">jboss.jca:service=RARDeployment,name='activemq-ra.rar'</depends>
      <attribute name="Type">javax.jms.Topic</attribute>
      <attribute name="Properties">PhysicalName=topic.inbound</attribute>
   </mbean>

</connection-factories>
```

This XML instance configures a JMS `QueueConnectionFactory` and a JMS `TopicConnectionFactory` and makes them available via JNDI. Also defined in this file are some {{AdminObject}}s which are used to specify a topic and a queue. This file should be dropped into the JBoss deploy directory. Its name (*-ds.xml) will cause it to be picked up by the JBoss deployer upon startup. Speaking of which, once this file is in place, a quick smoke test can be performed by simply starting up the JBoss server. Below is an example of the output that should be seen:

```
=========================================================================

  JBoss Bootstrap Environment

  JBOSS_HOME: /opt/jboss

  JAVA: java

  JAVA_OPTS: -server -Xms128m -Xmx512m -Dsun.rmi.dgc.client.gcInterval=3600000 
-Dsun.rmi.dgc.server.gcInterval=3600000 -Dprogram.name=run.sh

  CLASSPATH: /opt/jboss/bin/run.jar:/lib/tools.jar

=========================================================================

22:55:48,686 INFO  [Server] Starting JBoss (MX MicroKernel)...
22:55:48,688 INFO  [Server] Release ID: JBoss [Zion] 4.0.4.GA (build: CVSTag=JBoss\_4\_0\_4\_GA date=200605151000)
22:55:48,690 INFO  [Server] Home Dir: /opt/jboss-4.0.4.GA
22:55:48,692 INFO  [Server] Home URL: file:/opt/jboss-4.0.4.GA/
22:55:48,695 INFO  [Server] Patch URL: null
22:55:48,697 INFO  [Server] Server Name: default
22:55:48,698 INFO  [Server] Server Home Dir: /opt/jboss-4.0.4.GA/server/default
22:55:48,701 INFO  [Server] Server Home URL: file:/opt/jboss-4.0.4.GA/server/default/
22:55:48,702 INFO  [Server] Server Log Dir: /opt/jboss-4.0.4.GA/server/default/log
22:55:48,704 INFO  [Server] Server Temp Dir: /opt/jboss-4.0.4.GA/server/default/tmp
22:55:48,706 INFO  [Server] Root Deployment Filename: jboss-service.xml
22:55:49,092 INFO  [ServerInfo] Java version: 1.5.0_06,Apple Computer, Inc.
22:55:49,101 INFO  [ServerInfo] Java VM: Java HotSpot(TM) Server VM 1.5.0_06-64,"Apple Computer, Inc."
22:55:49,102 INFO  [ServerInfo] OS-System: Mac OS X 10.4.8,i386
22:55:49,803 INFO  [Server] Core system initialized
22:55:53,379 INFO  [WebService] Using RMI server codebase: http://rattlesnake:8083/
22:55:53,400 INFO  [Log4jService$URLWatchTimerTask] Configuring from URL: resource:log4j.xml
22:55:54,034 INFO  [NamingService] JNDI bootstrap JNP=/0.0.0.0:1099, RMI=/0.0.0.0:1098, backlog=50, 
no client SocketFactory, Server SocketFactory=class
org.jboss.net.sockets.DefaultSocketFactory
22:55:58,475 INFO  [Embedded] Catalina naming disabled
22:55:58,566 INFO  [ClusterRuleSetFactory] Unable to find a cluster rule set in the classpath. 
Will load the default rule set.
22:55:58,569 INFO  [ClusterRuleSetFactory] Unable to find a cluster rule set in the classpath. 
Will load the default rule set.
22:55:59,110 INFO  [Http11BaseProtocol] Initializing Coyote HTTP/1.1 on http-0.0.0.0-8080
22:55:59,114 INFO  [Catalina] Initialization processed in 545 ms
22:55:59,116 INFO  [StandardService] Starting service jboss.web
22:55:59,121 INFO  [StandardEngine] Starting Servlet Engine: Apache Tomcat/5.5.17
22:55:59,179 INFO  [StandardHost] XML validation disabled
22:55:59,221 INFO  [Catalina] Server startup in 105 ms
22:55:59,600 INFO  [TomcatDeployer] deploy, ctxPath=/invoker, warUrl=.../deploy/http-invoker.sar/invoker.war/
22:55:59,874 INFO  [WebappLoader] Dual registration of jndi stream handler: factory already defined
22:56:00,899 INFO  [TomcatDeployer] deploy, ctxPath=/, warUrl=.../deploy/jbossweb-tomcat55.sar/ROOT.war/
22:56:01,700 INFO  [TomcatDeployer] deploy, ctxPath=/jbossws, warUrl=.../tmp/deploy/tmp60528jbossws-exp.war/
22:56:01,891 INFO  [SubscriptionManager] Bound event dispatcher to java:/EventDispatcher
22:56:02,203 INFO  [TomcatDeployer] deploy, ctxPath=/jbossmq-httpil, 
warUrl=.../deploy/jms/jbossmq-httpil.sar/jbossmq-httpil.war/
22:56:04,546 INFO  [TomcatDeployer] deploy, ctxPath=/web-console, 
warUrl=.../deploy/management/console-mgr.sar/web-console.war/
22:56:05,690 INFO  [MailService] Mail Service bound to java:/Mail
22:56:07,215 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/activemq-ra.rar/
22:56:07,452 INFO  [XBeanXmlBeanDefinitionReader] Loading XML bean definitions from class 
path resource [broker-config.xml]
22:56:07,750 INFO  [ClassPathXmlApplicationContext] Bean factory for application context 
[org.apache.xbean.spring.context.ClassPathXmlApplicationContext;hashCode=13887543]:
org.springframework.beans.factory.support.DefaultListableBeanFactory defining
beans [org.apache.activemq.xbean.XBeanBrokerService]; root of BeanFactory hierarchy
22:56:07,765 INFO  [ClassPathXmlApplicationContext] 1 beans defined in application context
[org.apache.xbean.spring.context.ClassPathXmlApplicationContext;hashCode=13887543]
22:56:07,773 INFO  [CollectionFactory] JDK 1.4+ collections available
22:56:07,778 INFO  [ClassPathXmlApplicationContext] Unable to locate MessageSource with name 
'messageSource': using default
[org.springframework.context.support.DelegatingMessageSource@5fee96]
22:56:07,781 INFO  [ClassPathXmlApplicationContext] Unable to locate ApplicationEventMulticaster 
with name 'applicationEventMulticaster': using default
[org.springframework.context.event.SimpleApplicationEventMulticaster@78c714]
22:56:07,783 INFO  [DefaultListableBeanFactory] Pre-instantiating singletons in factory
 [org.springframework.beans.factory.support.DefaultListableBeanFactory
defining beans [org.apache.activemq.xbean.XBeanBrokerService]; root of BeanFactory hierarchy]
22:56:08,181 INFO  [BrokerService] ActiveMQ 4.0.2 JMS Message Broker (bruce.broker1) is starting
22:56:08,181 INFO  [BrokerService] For help or more information please see: 
http://incubator.apache.org/activemq/
22:56:09,989 INFO  [JDBCPersistenceAdapter] Database driver recognized: 
[apache\_derby\_embedded\_jdbc\_driver]
22:56:11,026 INFO  [JournalPersistenceAdapter] Journal Recovery Started from: Active Journal: 
using 5 x 20.0 Megs at: /opt/jboss-4.0.4.GA/activemq-data/journal
22:56:11,169 INFO  [JournalPersistenceAdapter] Journal Recovered: 0 message(s) in transactions recovered.
22:56:11,489 INFO  [TransportServerThreadSupport] Listening for connections at: tcp://rattlesnake:61616
22:56:11,491 WARN  [MulticastDiscoveryAgent] brokerName not set
22:56:11,519 INFO  [TransportConnector] Connector bruce.broker1 Started
22:56:11,522 INFO  [NetworkConnector] Network Connector bridge Started
22:56:11,522 INFO  [BrokerService] ActiveMQ JMS Message Broker 
(bruce.broker1, ID:rattlesnake-59052-1161060967859-1:0) started
22:56:11,556 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/jboss-ha-local-jdbc.rar
22:56:11,599 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/jboss-ha-xa-jdbc.rar
22:56:11,623 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/jboss-local-jdbc.rar
22:56:11,647 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/jboss-xa-jdbc.rar
22:56:11,737 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/jms/jms-ra.rar
22:56:11,847 INFO  [RARDeployment] Required license terms exist, view META-INF/ra.xml in 
.../deploy/mail-ra.rar
22:56:12,251 INFO  [ConnectionFactoryBindingService] Bound ConnectionManager 
'jboss.jca:service=ConnectionFactoryBinding,name=activemq/QueueConnectionFactory' to
JNDI name 'java:activemq/QueueConnectionFactory'
22:56:12,258 INFO  [ConnectionFactoryBindingService] Bound ConnectionManager 
'jboss.jca:service=ConnectionFactoryBinding,name=activemq/TopicConnectionFactory' to
JNDI name 'java:activemq/TopicConnectionFactory'
22:56:12,265 INFO  [AdminObject] Bound admin object 'org.apache.activemq.command.ActiveMQQueue' 
at 'activemq/queue/outbound'
22:56:12,330 INFO  [AdminObject] Bound admin object 'org.apache.activemq.command.ActiveMQTopic' 
at 'activemq/topic/inbound'
22:56:13,246 INFO  [ConnectionFactoryBindingService] Bound ConnectionManager 
'jboss.jca:service=DataSourceBinding,name=DefaultDS' to JNDI name 'java:DefaultDS'
22:56:13,842 INFO  [A] Bound to JNDI name: queue/A
22:56:13,845 INFO  [B] Bound to JNDI name: queue/B
22:56:13,846 INFO  [C] Bound to JNDI name: queue/C
22:56:13,848 INFO  [D] Bound to JNDI name: queue/D
22:56:13,850 INFO  [ex] Bound to JNDI name: queue/ex
22:56:13,876 INFO  [testTopic] Bound to JNDI name: topic/testTopic
22:56:13,879 INFO  [securedTopic] Bound to JNDI name: topic/securedTopic
22:56:13,880 INFO  [testDurableTopic] Bound to JNDI name: topic/testDurableTopic
22:56:13,883 INFO  [testQueue] Bound to JNDI name: queue/testQueue
22:56:13,994 INFO  [UILServerILService] JBossMQ UIL service available at : /0.0.0.0:8093
22:56:14,055 INFO  [DLQ] Bound to JNDI name: queue/DLQ
22:56:14,375 INFO  [ConnectionFactoryBindingService] Bound ConnectionManager 
'jboss.jca:service=ConnectionFactoryBinding,name=JmsXA' to JNDI name 'java:JmsXA'
22:56:14,525 INFO  [TomcatDeployer] deploy, ctxPath=/jmx-console, warUrl=.../deploy/jmx-console.war/
22:56:14,991 INFO  [Http11BaseProtocol] Starting Coyote HTTP/1.1 on http-0.0.0.0-8080
22:56:15,071 INFO  [ChannelSocket] JK: ajp13 listening on /0.0.0.0:8009
22:56:15,082 INFO  [JkMain] Jk running ID=0 time=0/25  config=null
22:56:15,108 INFO  [Server] JBoss (MX MicroKernel) [4.0.4.GA (build: CVSTag=JBoss\_4\_0\_4\_GA date=200605151000)] 
Started in 26s:398ms
```

Note the startup messages from both ActiveMQ and from the `AdminObjects` creating an `ActiveMQQueue` and an `ActiveMQTopic`. These are good indications that the configuration is correct, but needs to be verified a bit further. This is covered in the next section.

Testing the Integration
-----------------------

The seventh and final step is to perform a slightly more comprehensive smoke test of the integration. This can be accomplished using Apache Ant via the examples that come with the ActiveMQ binary distribution. An Ant build.xml file is included which provides easy access to a simple consumer and a simple producer. The producer will be used to send messages that are received by the consumer. To proceed with this testing, just follow the steps below:

1.  In the first terminal, start up JBoss. The same startup script can be used here as was used above.
2.  In the second terminal, use the commands below to run the ActiveMQ consumer:
    ```
    $ cd /opt/incubator-activemq-4.0.2/examples    #note in activemq v5 the directory is just "example" not "examples"
    $ ant consumer
    Buildfile: build.xml
    
    init:
        [mkdir] Created dir: /opt/incubator-activemq-4.0.2/example/target/classes
        [mkdir] Created dir: /opt/incubator-activemq-4.0.2/example/src/ddl
    
    compile:
        [javac] Compiling 6 source files to /opt/incubator-activemq-4.0.2/example/target/classes
    
    consumer:
         [echo] Running consumer against server at $url = tcp://localhost:61616 for subject $subject = TEST.FOO
         [java] Connecting to URL: tcp://localhost:61616
         [java] Consuming queue: TEST.FOO
         [java] Using non-durable subscription
         [java] We are about to wait until we consume: 10 message(s) then we will shutdown
    ```
3.  In the third terminal, use the commands below to run the ActiveMQ producer:
    ```
    $ cd /opt/incubator-activemq-4.0.2/examples
    $ ant producer
    Buildfile: build.xml
    
    init:
    
    compile:
    
    producer:
         [echo] Running producer against server at $url = tcp://localhost:61616 for subject $subject = TEST.FOO
         [java] Connecting to URL: tcp://localhost:61616
         [java] Publishing a Message with size 1000 to queue: TEST.FOO
         [java] Using non-durable publishing
         [java] Sleeping between publish 0 ms
         [java] Sending message: Message: 0 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 1 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 2 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 3 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 4 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 5 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 6 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 7 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 8 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Sending message: Message: 9 sent at: Mon Oct 16 23:21:08 MDT 2006  ...
         [java] Done.
         [java] connection {
         [java]   session {
         [java]     messageCount{ count: 10 unit: count startTime: 1161062468001 lastSampleTime: 
    1161062468340 description: Number of messages exchanged }
         [java]     messageRateTime{ count: 10 maxTime: 142 minTime: 1 totalTime: 315 averageTime: 31.5 
    averageTimeExMinMax: 21.5 averagePerSecond: 31.746031746031747
    averagePerSecondExMinMax: 46.51162790697674 unit: millis startTime: 1161062468002 lastSampleTime: 
    1161062468340 description: Time taken to process a message (thoughtput rate) }
         [java]     pendingMessageCount{ count: 0 unit: count startTime: 1161062468001 lastSampleTime: 
    1161062468001 description: Number of pending messages }
         [java]     expiredMessageCount{ count: 0 unit: count startTime: 1161062468001 lastSampleTime: 
    1161062468001 description: Number of expired messages }
         [java]     messageWaitTime{ count: 0 maxTime: 0 minTime: 0 totalTime: 0 averageTime: 0.0 
    averageTimeExMinMax: 0.0 averagePerSecond: 0.0
    averagePerSecondExMinMax: 0.0 unit: millis startTime: 1161062468002 lastSampleTime: 1161062468002 
    description: Time spent by a message before being delivered }
         [java]     durableSubscriptionCount{ count: 0 unit: count startTime: 1161062468002 lastSampleTime: 
    1161062468002 description: The number of durable
    subscriptions }
    
         [java]     producers {
         [java]       producer queue://TEST.FOO {
         [java]         messageCount{ count: 10 unit: count startTime: 1161062468025 lastSampleTime: 
    1161062468340 description: Number of messages processed }
         [java]         messageRateTime{ count: 10 maxTime: 142 minTime: 1 totalTime: 315 averageTime: 
    31.5 averageTimeExMinMax: 21.5 averagePerSecond:
    31.746031746031747 averagePerSecondExMinMax: 46.51162790697674 unit: millis startTime: 1161062468025 
    lastSampleTime: 1161062468340 description: Time taken to
    process a message (thoughtput rate) }
         [java]         pendingMessageCount{ count: 0 unit: count startTime: 1161062468025 lastSampleTime: 
    1161062468025 description: Number of pending messages }
         [java]         messageRateTime{ count: 10 maxTime: 142 minTime: 1 totalTime: 315 averageTime: 31.5 
    averageTimeExMinMax: 21.5 averagePerSecond:
    31.746031746031747 averagePerSecondExMinMax: 46.51162790697674 unit: millis startTime: 1161062468025 
    lastSampleTime: 1161062468340 description: Time taken to
    process a message (thoughtput rate) }
         [java]         expiredMessageCount{ count: 0 unit: count startTime: 1161062468025 lastSampleTime: 
    1161062468025 description: Number of expired messages }
         [java]         messageWaitTime{ count: 0 maxTime: 0 minTime: 0 totalTime: 0 averageTime: 0.0 
    averageTimeExMinMax: 0.0 averagePerSecond: 0.0
    averagePerSecondExMinMax: 0.0 unit: millis startTime: 1161062468025 lastSampleTime: 1161062468025 
    description: Time spent by a message before being delivered }
         [java]       }
         [java]     }
         [java]     consumers {
         [java]     }
         [java]   }
         [java] }
    ```

Step 1 above just starts up JBoss. Step 2 above starts up a simple message consumer that comes with ActiveMQ. Step 3 above starts up a simple message producer that comes with ActiveMQ. Though the message consumer and message producer are simple utilities, the each one accepts many parameters making them extremely useful for testing ActiveMQ configurations.

To paraphrase, what just happened was that the message producer sent 10 messages to the TEST.FOO destination and the message consumer received 10 messages from the TEST.FOO destination. Despite being a simple test, it does utilize the ActiveMQ broker, albeit only on a single machine. The next logical step is to set up a full network of ActiveMQ brokers.

After setting up one broker within one instance of JBoss, setting up another is made much easier, but requires another machine or operating system instance. But that's a whole separate article and something to address another day.

Conclusion
----------

What has been demonstrated here is the integration of ActiveMQ with the JBoss application server. This integration is quite common and performed by many enterprises. I hope that this was helpful to people interested in the integration of ActiveMQ with JBoss application server. If you have any questions or are interested in consulting services surrounding ActiveMQ, please [contact us](http://www.logicblaze.com/contact.jsp) for more information.

### Resources

Below are the configurations for use with both Spring 1.x and Spring 2.x:
  

[File](integrating-apache-activemq-classic-with-jboss.md?sortBy=name&sortOrder=ascending)

[Modified](integrating-apache-activemq-classic-with-jboss.md?sortBy=date&sortOrder=descending)

File amq-spring-1.2.6.tgz

Oct 03, 2007 by [Bruce Snyder](https://cwiki.apache.org/confluence/display/~bsnyder)

Labels

*   No labels
*   [Edit Labels](# "Edit Labels")

Preview

File amq-spring-2.0.tgz

Oct 03, 2007 by [Bruce Snyder](https://cwiki.apache.org/confluence/display/~bsnyder)

Labels

*   No labels
*   [Edit Labels](# "Edit Labels")

Preview

Download All

