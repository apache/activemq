---
title: "J2EE"
slug: /j2ee
---


In J2EE 1.4 or later the standard way to integrate with a JMS provider is via JCA 1.5 and a [Resource Adapter](resource-adapter).

Introduction
------------

ActiveMQ includes a Java Connector Architecture (JCA) 1.5 Resource Adapter. JCA 1.5 defines the contract between an J2EE application server and external resources such as databases and messaging middleware. It allows the application server to efficiently pool connections, control transactions and manage security. The Resource Adapter allows ActiveMQ to be used from any J2EE 1.4 application server. We have tested that the Resource Adapter works in the following J2EE 1.4 containers

*   [TomEE](http://tomee.apache.org/tomcat-jms) 1
*   Geronimo 1
*   GlassFish
*   JBoss 4
*   WebLogic 9
*   WebSphere 6

Features
--------

*   Inbound connection delivers messages to MDBs via XA or local transactions.
*   Outbound connections support standard container pooling or can reuse the inbound connection/session to avoid XA.
*   JTA support: Can be enlisted in XA and local transactions.
*   XA transaction recovery via XAResource.recover() supported.
*   When used outside a JTA transaction, session transaction settings retain normal JMS semantics so that it be used by your web-app tier.
*   Can configure and start up embedded broker.
*   Can connect to external ActiveMQ broker or embedded broker.
*   Inbound message delivery supports fine grain control of concurrency and prefetching.
*   Batching so that multiple messages can be delivered within the same transaction for optimal performances.

Downloading the RAR
-------------------

The RAR is available via [maven central](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22activemq-rar%22)

Deployment Guides

*   [TomEE](tomee)
*   [Geronimo](geronimo)
*   [Integrating Apache ActiveMQ with Glassfish](integrating-apache-activemq-classic-with-glassfish)
*   [JBoss Integration](jboss-integration)

The use of an Embedded Broker
-----------------------------

The ActiveMQ Resource Adapter can connect to a remote broker using any of the available transports, or it can start up an embedded broker. As described in the [Resource Adapter Properties](resource-adapter-properties), you can enable an embedded broker using the **BrokerXmlConfig** property.

Configuration Reference
-----------------------

*   [Resource Adapter Properties](resource-adapter-properties)
*   [Connection Factory Properties](connection-factory-properties)
*   [Activation Spec Properties](activation-spec-properties)

