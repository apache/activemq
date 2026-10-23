---
title: "JCA Container"
slug: /jca-container
---


The JCA Container is moving

We will continune to support the ActiveMQ JCA Container up until ActiveMQ 3.1.

After that point we will be moving to [Jencks](http://jencks.codehaus.org/) which is a migration of the ActiveMQ codebase together with code [Geronimo](http://geronimo.apache.org) and from some other contributors.

Jencks is completely logically equivalent to the ActiveMQ JCA Container and migrating to it is pretty simple (mostly just a class name change for the JCA container) - though the benefits of Jencks are that it supports full XA recovery and works well with Geronimo's TransactionManager and WorkManager.

So we do recommend you migrate to [Jencks](http://jencks.codehaus.org/) whenever its easy for you to do so; projects such as [Lingo](http://lingo.codehaus.org/) and [ServiceMix](http://servicemix.org/) already have and it was an easy migration.

We have a lightweight, easily embeddable Spring based JCA container which allows us to provide MDB like functionality inside any Java application without requiring a full EJB container.

This allows us to support _message driven pojos_ using dependency injection for efficient JMS consumption together with pooling of the pojos using lightweight containers rather than relying on EJB.

The JCA container also makes it easy to programatically at runtime create new message driven pojos rather than relying on the fixed deployment-time only option with EJB.

Example
-------

Here is [an example](http://docs.codehaus.org/download/attachments/23776/spring.xml) of the Spring XML to deploy a pojo (EchoBean) on an inbound JMS subscription, on a topic in this particular example.

Firstly we can create as many JCAContainer instances as we need; currently we create one per JCA [Resource Adapter](resource-adapter) (i.e. JMS provider). The JCAContainer also uses a WorkManager which is JCA speak for a bunch of thread pools. We can share the WorkManager across JCAContainer instances or create one per JCAContainer.

Once we have a JCAContainer we can add as many JCAConnector instances to it, via the **addConnector** factory method, each one representing a JMS subscription and providing a pool of POJOs to process the message. The subscription details are specified by an activationSpec property which is a bean typically dependent on the JMS provider; this allows providers to add new extensions while leaving your application code pure JMS.

Notice that the regular Spring pooling mechanism, the **targetSource** property, is used to pool the actual POJO and that Spring uses Dependency Injection to construct instances of the POJO.

**Note** you must set the **singleton** flag to false for the POJOs if they are not thread safe.

Requirements
------------

To be able to use the JCA container, you just need the following jars on your classpath

*   [required ActiveMQ jars](initial-configuration) if you are using ActiveMQ as your JMS provider - or your provider's jars if not
*   activemq-container.jar
*   spring.jar
*   J2EE.jar (for the JCA APIs). If you are inside Tomcat, it doesn't like j2ee.jar on the classpath, so use the individual jars from Geronimo - e.g. geronimo-spec-j2ee-connector-*.jar for the JCA APIs
*   commons-collections.jar
*   commons-pool.jar
*   aopalliance.jar (a temporary dependency introduced by Spring's use of the TargetSource, we should be able to remove this dependency later on).

`Note** the classes and resources in activemq-container.jar are not included in activemq.jar

To use the JCA Container please use the 2.x code release. Several problems were found and fixed with the 1.x branch of code.

Things to watch
---------------

By default the ActiveMQ [Resource Adapter](resource-adapter) will try to connect to a remote broker (ie. tcp://localhost:61616). Also if you want to set how the broker is configured via XML then try the _brokerXmlConfig_ property.

_Note:_ In AMQ 3.x the default behavior is the ActiveMQ resource adapter will create an embedded broker

