---
title: "JMS to JMS Bridge"
slug: /jms-to-jms-bridge
---


> **Warning**, try Camel first!
> 
> Note that we recommend you look at using [Apache Camel](http://camel.apache.org/) for bridging ActiveMQ to or from any message broker (or indeed [any other technology, protocol or middleware](http://camel.apache.org/components.html)) as its much easier to:
> 
> *   keep things flexible; its very easy to map different queue/topic to one or more queues or topics on the other provider
> *   perform content based routing, filtering and other [Enterprise Integration Patterns](http://camel.apache.orgFeatures/enterprise-integration-patterns)
> *   allows you to work with [any technology, protocol or middleware](http://camel.apache.org/components.html), not just JMS providers
> 
> e.g. in your Spring XML file just add:
> ```
> <camelContext xmlns="http://camel.apache.org/schema/spring">
>   <route>
>     <from uri="mqseries:Foo.Bar"/>
>     <to uri="activemq:Cheese"/>
>   </route>
> </camelContext>
> ```

### Introduction

ActiveMQ provides bridging functionality to other JMS providers that implement the JMS 1.0.2 and above specification. A JMS bridge can be co-located with an ActiveMQ broker or run remotely. In order to support JMS 1.0.2 there is seperation between Queues and Topics.

temporary destinations and replyTo destinations in the inbound message exchanges are automatically handled, enabling an ActiveMQ service to handle a foreign JMS TopicRequestor or QueueResquestor exchanges.

### Properties

**JMS Bridge Topic Connector**

property name|default value|description
---|---|---
localTopicConnection|null|if set will be used to connect to ActiveMQ
localTopicConnectionFactory|null|used to initialize the ActiveMQ JMS Connection if localTopicConnection is not set
localClientId|null|set the id of the local connection
outboundClientId|null|set the id of the outbound connection
jndiLocalTemplate|Spring default template|used for locating the Connection Factory for the ActiveMQ Connection if the localTopicConnection or localTopicConnectionFactory is not set
outboundTopicConnection|null|if set will be used to connect to the foreign JMS provider
outboundTopicConnectionFactory|null|used to initialize the foreign JMS Connection if outboundTopicConnection is not set
jndiOutboundTemplate|Spring default template|used for locating the Connection Factory for the ActiveMQ Connection if the localTopicConnection or localTopicConnectionFactory is not set
localUsername|null|if set will be used for authentication to the ActiveMQ JMS provider
localPassword|null|if set will be used for authentication to the ActiveMQ JMS provider
outboundUsername|null|if set will be used for authentication to the foreign JMS provider
outboundPassword|null|if set will be used for authentication to the foreign JMS provider
inboundMessageConvertor|null|if set will be used for converting foreign JMS Messages to a format for ActiveMQ
outboundMessageConvertor|null|if set will be used for converting ActiveMQ messages to a format for the foriegn JMS provider
inboundTopicBridges|null|an array of InboundTopicBridge instances - used for defining inbound (subscribe to) traffic from the foreign JMS provider
outboundTopicBridges|null|an array of OutboundTopicBridge instances - used for defining destinations that will be published to the foreign JMS provider

**JMS Bridge Queue Connector**

property name|default value|description
---|---|---
localQueueConnection|null|if set will be used to connect to ActiveMQ
localQueueConnectionFactory|null|used to initialize the ActiveMQ JMS Connection if localQueueConnection is not set
localClientId|null|set the id of the local connection
outboundClientId|null|set the id of the outbound connection
jndiLocalTemplate|Spring default template|used for locating the Connection Factory for the ActiveMQ Connection if the localQueueConnection or localQueueConnectionFactory is not set
outboundQueueConnection|null|if set will be used to connect to the foreign JMS provider
outboundQueueConnectionFactory|null|used to initialize the foreign JMS Connection if localQueueConnection is not set
jndiOutboundTemplate|Spring default template|used for locating the Connection Factory for the ActiveMQ Connection if the localQueueConnection or localQueueConnectionFactory is not set
localUsername|null|if set will be used for authentication to the ActiveMQ JMS provider
localPassword|null|if set will be used for authentication to the ActiveMQ JMS provider
outboundUsername|null|if set will be used for authentication to the foreign JMS provider
outboundPassword|null|if set will be used for authentication to the foreign JMS provider
inboundMessageConvertor|null|if set will be used for converting foreign JMS Messages to a format for ActiveMQ
outboundMessageConvertor|null|if set will be used for converting ActiveMQ messages to a format for the foriegn JMS provider
inboundQueueBridges|null|an array of InboundQueueBridge instances - used for defining inbound (subscribe to) traffic from the foreign JMS provider
outboundQueueBridges|null|an array of OutboundQueueBridge instances - used for defining destinations that will be forwarded to the foreign JMS provider

#### Topic Bridges

**InboundTopicBridge**

property name|default value|description
---|---|---
localTopicName|null|the name of the local ActiveMQ Queue
inboundTopicName|null|the foreign topic name to subscribe to
selector|null|selector to use - if any
consumerName|null|if set will create a durable consumer

**OutboundTopicBridge**

property name|default value|description
---|---|---
localTopicName|null|the name of the local ActiveMQ Queue
outboundTopicName|null|the foreign topic name to publish to

#### Queue Bridges

**InboundQueueBridge**

property name|default value|description
---|---|---
localQueueName|null|the name of the local ActiveMQ Queue
inboundQueueName|null|the foreign queue name to receive from
selector|null|selector to use - if any

  
**OutboundQueueBridge**

property name|default value|description
---|---|---
localQueueName|null|the name of the local ActiveMQ Queue
outboundQueueName|null|the foreign queue name to send to

### Example XBean Configuration

The following [example config file](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/org/apache/activemq/network/jms/queue-xbean.xml) shows how to use the regular [Xml Configuration](xml-configuration) to configure a JMS to JMS bridge.
```xml
<broker xmlns="http://activemq.apache.org/schema/core" id="localbroker"
  brokerName="localBroker" persistent="false">
  <jmsBridgeConnectors>
    <jmsQueueConnector
      outboundQueueConnectionFactory="#remoteFactory">
      <inboundQueueBridges>
        <inboundQueueBridge
          inboundQueueName="org.apache.activemq.network.jms.QueueBridgeXBeanTest" />
      </inboundQueueBridges>
    </jmsQueueConnector>
  </jmsBridgeConnectors>
  <transportConnectors>
    <transportConnector uri="tcp://localhost:61234" />
  </transportConnectors>
</broker>
<!-- JMS ConnectionFactory to use remote -->
<bean id="remoteFactory"
  class="org.apache.activemq.ActiveMQConnectionFactory">
  <property name="brokerURL" value="tcp://localhost:61666" />
</bean>
```

### Example pure Spring Configuration

The following example shows how to use raw Spring XML to wire together a broker - bridging to a Foreign JMS provider
```xml
<!-- local broker with embedded Jms to Jms bridge (ok - it's contrived) -->
<bean id="localbroker" class="org.apache.activemq.broker.BrokerService"
  init-method="start">
  <property name="brokerName" value = "localBroker"/>
    <property name="persistent" value = "false"/>
  <property name="transportConnectorURIs">
    <list>
      <value>tcp://localhost:61234</value>
    </list>
  </property>
  <property name="jmsBridgeConnectors">
    <list>
      <ref bean="jmsConnector"/>
    </list>
  </property>
</bean>

<!-- JMS ConnectionFactory to use local broker (the one with the bridge) -->
<bean id="localFactory"
  class="org.apache.activemq.ActiveMQConnectionFactory">
  <property name="brokerURL" value="tcp://localhost:61234" />
</bean>

<!--JmsTopicConnector - the Jms bridge -->
<bean id="jmsConnector"
  class="org.apache.activemq.network.jms.JmsTopicConnector">
  <property name = "outboundTopicConnectionFactory" ref = "remoteFactory"/>

  <property name = "inboundTopicBridges">
    <list>
      <ref bean="InboundTopicBridge" />
    </list>
  </property>

</bean>

<bean id ="InboundTopicBridge" class="org.apache.activemq.network.jms.InboundTopicBridge">
  <property name = "inboundTopicName" value = "org.apache.activemq.network.jms.TopicBridgeSpringTest"/>
</bean>
```

### Example XBean Configuration to Bridge ActiveMQ to Provider With No URL Setter

Some JMS providers, WebLogic for instance, do not expose a setter for connection properties like host and port (setBrokerUrl) on their ConnectionFactory object. In this case you need to set outboundQueueConnectionFactoryName and jndiOutboundTemplate in your activemq.xml config file.
```xml
<!-- START SNIPPET: example -->
<beans>

  <!-- Allows us to use system properties as variables in this configuration file -->
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

  <broker useJmx="true" xmlns="http://activemq.org/config/1.0">

    <persistenceAdapter>
        <journaledJDBC journalLogFiles="5" dataDirectory="${activemq.home}/activemq-data"/>
    </persistenceAdapter>

    <transportConnectors>
       <transportConnector name="default" uri="tcp://localhost:61616" discoveryUri="multicast://default"/>
       <transportConnector name="stomp"   uri="stomp://localhost:61613"/>
    </transportConnectors>

    <networkConnectors>
      <networkConnector name="default" uri="multicast://default"/>
    </networkConnectors>

    <jmsBridgeConnectors>
     <jmsQueueConnector name="JreportRequestBridge-Inbound"
        jndiOutboundTemplate="#remoteJndi"
        outboundQueueConnectionFactoryName="jms/ConnectionFactory"
        localQueueConnectionFactory="#localFactory">
        <inboundQueueBridges>
          <inboundQueueBridge inboundQueueName="jms/queue/jreport/request"/>
        </inboundQueueBridges>
      </jmsQueueConnector>
    </jmsBridgeConnectors>

  </broker>

    <!-- Set up the template for connecting to Weblogic -->
    <bean id="remoteJndi" class="org.springframework.jndi.JndiTemplate">
        <property name="environment">
                <props>
                        <prop key="java.naming.factory.initial">weblogic.jndi.WLInitialContextFactory</prop>
                        <prop key="java.naming.provider.url">t3://<your ip here>:7001</prop>
                </props>
        </property>
    </bean>

  <bean id="localFactory"
    class="org.apache.activemq.ActiveMQConnectionFactory">
    <property name="brokerURL" value="tcp://localhost:61616" />
  </bean>

  <bean id="localQueue" class="org.apache.activemq.command.ActiveMQQueue">
    <constructor-arg value="dynamic/jms.queue.jreport.request"/>
  </bean>
</beans>
<!-- END SNIPPET: xbean -->
```

### Example pure Spring Configuration for sending messages to external ActiveMQ destination through bridge

Spring beans:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE beans PUBLIC "-//SPRING//DTD BEAN//EN" "http://www.springframework.org/dtd/spring-beans.dtd">

<beans>

  <bean id="mainBroker" class="org.apache.activemq.broker.BrokerService" init-method="start" destroy-method="stop">
    <property name="brokerName" value = "mainBroker"/>
    <property name="persistent" value="false"/>
    <property name="transportConnectorURIs">
      <list>
        <value>tcp://localhost:7000</value>
      </list>
    </property>
  </bean>

  <bean id="bridgedBroker" class="org.apache.activemq.broker.BrokerService" init-method="start" destroy-method="stop">
    <property name="brokerName" value = "bridgedBroker"/>
    <property name="persistent" value="false"/>
    <property name="transportConnectorURIs">
      <list>
        <value>tcp://localhost:7001</value>
      </list>
    </property>
    <property name="jmsBridgeConnectors">
      <list>
        <bean class="org.apache.activemq.network.jms.JmsQueueConnector">
          <property name="outboundQueueConnectionFactory">
            <bean class="org.apache.activemq.ActiveMQConnectionFactory">
              <property name="brokerURL" value="tcp://localhost:7000" />
            </bean>
          </property>
          <property name="outboundQueueBridges">
            <list>
              <bean class="org.apache.activemq.network.jms.OutboundQueueBridge">
                <constructor-arg value="messages.input"/>
              </bean>
            </list>
          </property>
        </bean>
      </list>
    </property>
  </bean>

</beans>
```
Java code:
```
public class BridgeTest {

  public BridgeTest() throws Exception {

      Log log = LogFactory.getLog(getClass());

      new ClassPathXmlApplicationContext("bridge/context-bridge.xml");

      ActiveMQConnection connection = ActiveMQConnection.makeConnection("tcp://localhost:7001");
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createQueue("messages.input");
      MessageProducer producer = session.createProducer(destination);
      producer.send(session.createTextMessage("Test Message"));
      log.debug("send message");
      session.close();
      connection.close();

      connection = ActiveMQConnection.makeConnection("tcp://localhost:7000");
      connection.start();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      destination = session.createQueue("messages.input");
      MessageConsumer consumer = session.createConsumer(destination);
      log.debug("receive message");
      Message message = consumer.receive(5000);
      log.debug("Received: " + message);
      session.close();
      connection.close();
  }

  public static void main(String[] args) throws Exception {
    new BridgeTest();
  }

}
```
