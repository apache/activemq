---
title: "SJSAS with GenericJMSRA"
slug: /sjsas-with-genericjmsra
---


Using ActiveMQ, Generic JMS RA and SJSAS (Glassfish)
====================================================

This document is my notes on making ActiveMQ and SJSAS work together using GenericJMSRA. The objectives is to make ActiveMQ as the JMS provider and MDB can be deployed in SJSAS, listening messages from ActiveMQ. Please note that, the SJSAS version I use is 9.0 Update 1. I don't have time to test it with SJSAS 9.1 or Glassfish v2.

The procedures
--------------

First, download all binaries:

*   genericra 1.7 (rar)
*   activemq 4.1.1 (zip or tar.gz)

The activemq have some dependencies, however, you can easily found all of them in the distribution (the zip or tar.gz). The following is list of minimum dependencies:

*   activemq-core
*   activeio
*   commons-logging
*   backport-util-concurrent

In order to use genericra you need to first create a resource adapter config using asadmin (Command line tools of SJSAS).
```
asadmin create-resource-adapter-config
  --property
      SupportsXA=false
      :RMPolicy=OnePerPhysicalConnection
      :ProviderIntegrationMode=javabean
      :ConnectionFactoryClassName=org.apache.activemq.ActiveMQConnectionFactory
      :QueueConnectionFactoryClassName=org.apache.activemq.ActiveMQConnectionFactory
      :TopicConnectionFactoryClassName=org.apache.activemq.ActiveMQConnectionFactory
      :XAConnectionFactoryClassName=org.apache.activemq.ActiveMQXAConnectionFactory
      :XAQueueConnectionFactoryClassName=org.apache.activemq.ActiveMQXAConnectionFactory
      :XATopicConnectionFactoryClassName=org.apache.activemq.ActiveMQXAConnectionFactory
      :UnifiedDestinationClassName=org.apache.activemq.command.ActiveMQDestination
      :QueueClassName=org.apache.activemq.command.ActiveMQQueue
      :TopicClassName=org.apache.activemq.command.ActiveMQTopic
      :ConnectionFactoryProperties=brokerURL\\\=tcp\\\://127.0.0.1\\\:61616
      :LogLevel=FINE
  myapp#genericra
```
You should note that, the above command should executed as one single line and no space around ':'. Just like:
```
asadmin create-resource-adapter-config --property SupportsXA=false:ConnectionFactoryProperties=brokerURL=tcp://127.0.0.1:61616 myapp#genericra
```
Under DOS prompt, you should use ONLY ONE '/' to do espcape, like this.
```
asadmin create-resource-adapter-config --property SupportsXA=false:ConnectionFactoryProperties=brokerURL=tcp://127.0.0.1:61616 myapp#genericra
```
Package your application, genericra.rar and activemq dependencies in one single EAR. The structure of the EAR should like this:
```
lib/activemq-core-4.1.1.jar
lib/log4j-1.2.13.jar
lib/commons-logging-1.1.jar
lib/backport-util-concurrent-2.1.jar
lib/activeio-core-3.0.0-incubator.jar
META-INF/application.xml
genericra.rar
mymodules.jar
```
Inside the mymodules.jar (where I put my MDB), the sun-ejb-jar.xml should look like this:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE sun-ejb-jar
  PUBLIC "-//Sun Microsystems, Inc.//DTD Application Server 8.1 EJB 2.1//EN"
    "http://www.sun.com/software/appserver/dtds/sun-ejb-jar\_2\_1-1.dtd">
<sun-ejb-jar>
  <enterprise-beans>
    <ejb>
      <ejb-name>TestingMessageDrivenBean</ejb-name>
      <mdb-connection-factory>
        <jndi-name>jms/SimpleQueueConnectionFactory</jndi-name>
      </mdb-connection-factory>
      <mdb-resource-adapter>
        <resource-adapter-mid>myapp#genericra</resource-adapter-mid>
        <activation-config>
          <activation-config-property>
            <activation-config-property-name>DestinationType</activation-config-property-name>
            <activation-config-property-value>javax.jms.Queue</activation-config-property-value>
          </activation-config-property>
          <activation-config-property>
            <activation-config-property-name>DestinationProperties</activation-config-property-name>
            <activation-config-property-value>PhysicalName=Foo.Bar</activation-config-property-value>
          </activation-config-property>
        </activation-config>
      </mdb-resource-adapter>
    </ejb>
  </enterprise-beans>
</sun-ejb-jar>
```
And the application.xml should look like this:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<application
    xmlns="http://java.sun.com/xml/ns/javaee"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/application_5.xsd"
    version="5">
  <description>Example DD</description>
  <display-name>sample app with amq and genericra</display-name>
  <module>
    <ejb>mymodules.jar</ejb>
  </module>
  <module>
    <connector>genericra.rar</connector>
  </module>
</application>
```
Deploy the genericra:
```
asadmin deploy --name myapp myapp.ear
```
Create connection pool. Run the following command in one single line:
```
asadmin create-connector-connection-pool
  --raname myapp#genericra
  --connectiondefinition javax.jms.QueueConnectionFactory
  --transactionsupport LocalTransaction
  ActiveMQQueueConnectionFactoryPool
```
Create connection factory admin object. Run the following command in one single line, note that, "jms/SimpleQueueConnectionFactory" need to match in your sun-ejb-jar.xml:
```
asadmin create-connector-resource
  --poolname ActiveMQQueueConnectionFactoryPool
  jms/SimpleQueueConnectionFactory
```

Classloader / commons-logging+log4j issue
-----------------------------------------

In the procedure shown above, you should notice one thing, ie, the RA, ActiveMQ and my MDB is deployed as one single EAR. You can deploy the genericra standalone without putting it into the ear, provided that you are not using commons-logging and log4j (directly or indirectly).

In the application I'm working on, commons-logging is used, and ActiveMQ also use commons-logging. In SJSAS, connector classloader is parent of the application classloader, as a result, if you deploy genericra independently, and then you deploy your application, log4j.xml will never get loaded except you put your application log4j.xml along with the ActiveMQ jars.

With Java standard classloading procedure, classloader will first delegate the loading to parent. When application classloader lookup the LogFactory class, it will first delegate to its parent classloader (ie the connector classloader). As the connector classloader have loaded ActiveMQ and its dependencies (which include commons-logging), LogFactory will finally loaded by the connector classloader. When commons-logging try to initialize the LogFactoryImpl, and hence trigger the standard procedure of log4j initialization, log4j configuration (log4j.xml or log4j.properties) in your application will not get loaded (as log4j logger is loaded by connector classloader).

As a result, deploy genericra independently is not the perfect solution in this case.

Luckily, with some advice from Glassfish forum (thanks Sivakumar), I worked out a better solution, ie, packaged all things (including genericra and activemq) into one EAR. When packaged in this way, all classes will be loaded under one classloader, and the good thing is, you can put your log4j.xml within one of the your jar (ie, the normal way!).

However, you should note one minor point, deploying genericra standalone and packaged with the EAR have one minor difference. If you deploy the RA with EAR, you need to reference the RA using the format 'appName#raName'. As a result, the reference name of the RA is myapp#genericra (see create-resource-adapter-config and sun-ejb-jar.xml part).

If you deploy the RA independently, the ra name is just genericra (as shown in the example by Ramesh).

LocalTransaction
----------------

In the instruction described above, LocalTransaction is used. (SupportsXA=false and --transactionsupport LocalTransaction) as I am currently using Non-XA transaction, however, as example shown by Ramesh, XA should also work.

Configuring genericra
---------------------

When you create the resource-adapter-config, there is two way to configure. One way is using JNDI, another way is using JavaBean introspection feature of genericra. In the example above, I go with JavaBean introspection feature (so 'ProviderIntegrationMode=javabean'). For more details on this, please checkout the user guide of genericra. Also, I found that expample provided on the genericra website is also valuable resource.

Reference
---------

*   [Another example on using genericjmsra and activemq by Ramesh](http://weblogs.java.net/blog/rampsarathy/archive/2007/03/glassfish_v2_an_1.html)
*   [Official user guide on configuring genericjmsra](https://genericjmsra.dev.java.net/docs/userguide/userguide.html)
*   [The thread in Glassfish forum which I found my answer](http://forums.java.net/jive/thread.jspa?messageID=211849)

