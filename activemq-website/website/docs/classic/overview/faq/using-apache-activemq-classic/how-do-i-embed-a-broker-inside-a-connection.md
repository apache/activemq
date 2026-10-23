---
title: "How do I embed a Broker inside a Connection"
slug: /how-do-i-embed-a-broker-inside-a-connection
---

In many messaging topologies there are JMS Brokers (server side) and a JMS client side. Often it makes sense to deploy a broker within your JVM. This allows you to optimise away a network hop; making the networking of JMS as efficient as pure RMI, but with all the usual JMS features of location independence, reliability, load balancing etc.

There are various ways to embed a broker in ActiveMQ depending on if you are using Java, Spring, XBean or using the ActiveMQConnectionFactory .

### Using explicit Java code

The following Java code will create an embedded broker
```
BrokerService broker = new BrokerService();

// configure the broker
broker.addConnector("tcp://localhost:61616");

broker.start();
```
If you want to lazily bind the transport connector as part of start(), useful when start() will block pending a store lock (as in a slave start), you can use the following code
```
BrokerService broker = new BrokerService();

TransportConnector connector = new TransportConnector();
connector.setUri(new URI("tcp://localhost:61616"));
broker.addConnector(connector);
broker.start();
```
In the same JVM clients can then use the [vm:// transport](vm-transport-reference) to connect to the embedded broker - whilst external clients can use the [tcp:// protocol](tcp-transport-reference)

If you have more than one embedded broker, ensure that you give them a unique name and - e.g.
```
BrokerService broker = new BrokerService();
// configure the broker
broker.setBrokerName("fred");
broker.addConnector("tcp://localhost:61616");
broker.start();
```
Then if you want to connect to the broker named 'fred' from within the same JVM, you can by using the uri **vm://fred**

It is possible to fully configure a broker through application code e.g.
```
BrokerService broker = new BrokerService();
broker.setBrokerName("fred");
broker.setUseShutdownHook(false);
//Add plugin
broker.setPlugins(new BrokerPlugin[]{new JaasAuthenticationPlugin()});
//Add a network connection
NetworkConnector connector = answer.addNetworkConnector("static://"+"tcp://somehost:61616");
connector.setDuplex(true);
broker.addConnector("tcp://localhost:61616");
broker.start();
```
Please note that you should add plugins before connectors or they will not be initialized

For more details on the available properties you can specify, see the [BrokerService javadoc](http://activemq.apache.org/maven/5.11.0/apidocs/org/apache/activemq/broker/BrokerService.html)

### Using the BrokerFactory

There is a helper class called [BrokerFactory](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/broker/BrokerFactory.html) which can be used to create a broker via URI for configuration.
```
BrokerService broker = BrokerFactory.createBroker(new URI(someURI));
```
The available values of the URI are:

URI scheme|Example|Description
---|---|---
xbean:|bean:activemq.xml|Searches the classpath (and file system) for an XML document with the given URI (activemq.xml in this case) which will then be used as the [Xml Configuration](xml-configuration)
broker:|broker:tcp://localhost:61616|Uses the [Broker Configuration URI](broker-configuration-uri) to confgure the broker

### Using Spring

There is a factory bean that can refer to an external ActiveMQ XML configuration file
```
<bean id="broker" class="org.apache.activemq.xbean.BrokerFactoryBean">
  <property name="config" value="classpath:org/apache/activemq/xbean/activemq.xml" />
  <property name="start" value="true" />
</bean>
```
In this case the usual Spring `classpath:org/apache/activemq/xbean/activemq.xml` resource mechanism is being used so that the activemq.xml file would be found on the classpath by looking inside all the directories on the classpath then looking for `org/apache/activemq/xbean/activemq.xml`. You can of course change this to any value you like. e.g. use `classpath:activemq.xml` if you just want to drop it in a directory that is in the classpath; like `WEB-INF/classes` in a web application.

If you wish you can use a URL instead using the **file:* or *http:** prefixes. For more details see how [Spring deals with resources](http://static.springframework.org/spring/docs/1.2.x/reference/beans.html#context-functionality-resources)

### Using XBean

If you are already using [XBean](http://geronimo.apache.org/xbean/) then you can just mix and match your Spring/XBean [XML configuration](https://github.com/apache/activemq/tree/main/activemq-core/src/test/resources/org/apache/activemq/xbean/activemq.xml) with ActiveMQ's configuration.
```
<beans 
  xmlns="http://www.springframework.org/schema/beans" 
  xmlns:amq="http://activemq.apache.org/schema/core"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

  <broker useJmx="true" xmlns="http://activemq.apache.org/schema/core">

    <persistenceFactory>
      <kahaDB directory="${basedir}/target" />
    </persistenceFactory>

    <transportConnectors>
      <transportConnector uri="tcp://localhost:61636" />
    </transportConnectors>

  </broker>
</beans>
```

### Using Spring 2.0

If you are using Spring 2.0 and ActiveMQ 4.1 or later (and xbean-spring 2.5 or later) you can embed the ActiveMQ broker XML inside any regular Spring.xml file without requiring the above factory bean. e.g. here is an [example](http://svn.apache.org/repos/asf/incubator/activemq/trunk/activemq-core/src/test/resources/spring-embedded-xbean.xml) of a regular Spring XML file in Spring 2.0 which also configures a broker.
```
<beans 
  xmlns="http://www.springframework.org/schema/beans" 
  xmlns:amq="http://activemq.apache.org/schema/core"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">
  
  <!--  lets create an embedded ActiveMQ Broker -->
  <amq:broker useJmx="false" persistent="false">
    <amq:transportConnectors>
      <amq:transportConnector uri="tcp://localhost:0" />
    </amq:transportConnectors>
  </amq:broker>

   <!--  ActiveMQ destinations to use  -->
  <amq:queue id="destination"  physicalName="org.apache.activemq.spring.Test.spring.embedded"/>

  <!-- JMS ConnectionFactory to use, configuring the embedded broker using XML -->
  <amq:connectionFactory id="jmsFactory" brokerURL="vm://localhost"/>
  

  <!-- Spring JMS Template -->
  <bean id="myJmsTemplate" class="org.springframework.jms.core.JmsTemplate">
    <property name="connectionFactory">
      <!-- lets wrap in a pool to avoid creating a connection per send -->
      <bean class="org.springframework.jms.connection.SingleConnectionFactory">
        <property name="targetConnectionFactory">
          <ref local="jmsFactory" />
        </property>
      </bean>
    </property>
  </bean>

  <bean id="consumerJmsTemplate" class="org.springframework.jms.core.JmsTemplate">
    <property name="connectionFactory" ref="jmsFactory"/>
  </bean>

  <!-- a sample POJO which uses a Spring JmsTemplate -->
  <bean id="producer" class="org.apache.activemq.spring.SpringProducer">
    <property name="template">
      <ref bean="myJmsTemplate"></ref>
    </property>

    <property name="destination">
      <ref bean="destination" />
    </property>

    <property name="messageCount">
      <value>10</value>
    </property>
  </bean>

  <!-- a sample POJO consumer -->
  <bean id="consumer" class="org.apache.activemq.spring.SpringConsumer">
    <property name="template" ref="consumerJmsTemplate"/>
    <property name="destination" ref="destination"/>
  </bean>

</beans>
```

### Using ActiveMQConnectionFactory

An embedded broker can also be created using an ActiveMQConnectionFactory and using a vm connector as a uri. e.g.
```
ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
```
Use the query parameters "broker.<property>" to configure the broker, where <property> matches the bean properties on the BrokerService.

The broker will be created upon creation of the first connection.

You can turn off auto creation by setting the create property on the VM Transport to false:
```
ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
```
