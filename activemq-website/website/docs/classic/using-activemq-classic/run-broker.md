---
title: "Run Broker"
description: "Run an ActiveMQ 4.x broker as a Unix service, with the Java Service Wrapper, in a servlet engine, J2EE 1.4 server or Maven, and reconnect clients via failover."
slug: /run-broker
displayed_sidebar: docs
---


Running an ActiveMQ Broker
==========================

Note if you want to use an **embedded broker** then see [How do I embed a Broker inside a Connection](how-do-i-embed-a-broker-inside-a-connection)

This page describes how to run a broker using 4.x or later of ActiveMQ.

### Running the broker as a Unix Service

See the  [Unix Shell Script](unix-shell-script) for details.

### Running the broker using the Java Service Wrapper Implementation

See the [Java Service Wrapper Page](java-service-wrapper) for details.

### Monitoring the broker

You can monitor ActiveMQ using the [Web Console](web-console) by pointing your browser at

[http://localhost:8161/admin](http://localhost:8161/admin)

From ActiveMQ 5.8 onwards the web apps is secured out of the box. 
 
The default username and password is admin/admin. You can configure this in the `conf/jetty-real.properties` file.

Or you can use the [JMX](jmx) support to view the running state of ActiveMQ.

For more information see the file `docs/WebConsole-README.txt` in the distribution.

### Running the broker inside a Servlet Engine

See the source code (or WAR) of the [Web Console](web-console) for an example of how to run the broker inside a web application using Spring.

### Running the broker inside your J2EE 1.4 Application Server

Whether its Apache Geronmio, JBoss 4, WebLogic 9 or some other J2EE 1.4 container you should be able to just reconfigure and then deploy the activemq-*.rar which is included in the binary distribution as a deployment unit in your app server. By default the rar is not configured to start an embedded broker. But by setting the brokerXmlConfig on the resource adapter configuration, the resource adapter will start an embedded broker.

For more details see [J2EE](j2ee)

### Running the broker from the source code

From the latest [checkout](source) of the code you can run a broker using the [ActiveMQ Performance Plugin](activemq-classic-performance-module-users-manual)

### Running the broker from maven

You can download and install the ActiveMQ Startup Maven Plugin via the following command if you are in a directory with a pom.xml. More detailed usage [here](maven2-activemq-broker-plugin)
```
mvn org.apache.activemq.tooling:maven-activemq-plugin:5.0-SNAPSHOT:run
```
You can also include it the pom and run it using:
```
mvn activemq:run
```

Handling JMS brokers going down
-------------------------------

A common requirement is that if the JMS broker goes down you want to automatically detect the failure and try to reconnect under the covers so that your application does not have to worry about reconnection.

There is detailed documentation on this in [Configuring Transports](configuring-transports); briefly...

Just change your connection URI in 4.x to
```
failover:tcp://host:port
```
And the JMS client will auto-reconnect to the broker if it is shutdown and restarted later on.

