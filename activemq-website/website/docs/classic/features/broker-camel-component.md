---
title: "Broker Camel Component"
slug: /broker-camel-component
---

Broker Camel Component
----------------------

**Available as of ActiveMQ 5.9**

Embedding Apache Camel inside the ActiveMQ broker provides great flexibility for extending the message broker with the integration power of Camel. Apache Camel routes also benefit in that you can avoid the serialization and network costs of connecting to ActiveMQ remotely - if you use the [activemq component](http://camel.apache.org/activemq.html).

If however, you want to change the behavior of messages flowing through the ActiveMQ message broker itself you will be limited to the shipped set of ActiveMQ broker [Interceptors](interceptors) - or develop your own [Broker plugin](developing-plugins) and then introduce that as a jar on to the class path for the ActiveMQ broker. The `broker` Camel component makes this even easier. It intercepts messages as they move through the broker itself, allowing them to be modified and manipulated before they are persisted to the message store or delivered to end consumers.

For example [by defining a CamelContext to run inside the broker's JVM](how-should-i-package-applications-using-camel-and-activemq-classic) the `broker` component can intercept all messages published to a Topic, say, and publish them to a Queue instead, changing their priority along the way:
```
<route id="setPriority">
   <from uri="broker:topic:test.broker.>"/>
      <setHeader headerName="JMSPriority">
         <constant>9</constant>
      </setHeader>
   <to uri="broker:queue:test.broker.component.queue"/>
</route>
```
Notes:

*   A broker component only adds an intercept into the broker if its started - so the broker component will not add any overhead to the running broker until its used - and then the overhead will be trivial.
*   Messages are intercepted by the broker component when they have been received by the broker - but before they are processed (persisted or routed to a destination).
*   The `IN` message on the Exchange is a `CamelMessage`, but also a JMS Message (messages routed through ActiveMQ from STOMP/MQTT/AMQP etc. are always translated into JMS messages).
*   [Wildcards](wildcards) can be used on a destination to intercept messages from destinations matching the wildcard.
*   After the intercept, you have to explicitly send the message back to the broker component - this allows you to either drop select messages (by not sending) - or, like in the above case - re-route the message to a different destination.  

There is one deliberate caveat though, only intercepted messages can be sent to a `broker` component. For example, routing a Camel message from another Component e.g. `file`, will result in an error.

Extra classes that have been added to the `activemq-broker` package to support the `broker` component. They allow the state of the running broker to be interrogated without using JMX. These classes are:

*   [org.apache.activemq.broker.view.MessageBrokerView](http://activemq.apache.org/maven/5.9.0/apidocs/org/apache/activemq/broker/view/MessageBrokerView.html) - provides methods to retrieve statistics on a the broker
*   From the `org.apache.activemq.broker.view.MessageBrokerView` - you can retrieve a [org.apache.activemq.broker.view.BrokerDestinationView](http://activemq.apache.org/maven/5.9.0/apidocs/org/apache/activemq/broker/view/BrokerDestinationView.html) for a particular destination.

### Example

How to route messages when a destination's queue depth has reached a certain limit:
```
<camelContext id="camel" trace="false" xmlns="http://camel.apache.org/schema/spring">
  <route id="routeAboveQueueLimitTest">
    <from uri="broker:queue:test.broker.queue"/>
    <choice>
      <when>
        <spel>#{@destinationView.queueSize >= 100}</spel>
        <to uri="broker:queue:test.broker.processLater"/>
      </when>
      <otherwise>
        <to uri="broker:queue:test.broker.queue"/>
      </otherwise>
    </choice>
  </route>
</camelContext>

<bean id="brokerView" class="org.apache.activemq.broker.view.MessageBrokerView">
  <constructor-arg value="testBroker"/>
</bean>

<bean id="destinationView" factory-bean="brokerView" factory-method="getDestinationView">
  <constructor-arg value="test.broker.component.route"/>
</bean>
```
This is using the Camel Message Router pattern. Note the use of the Spring expression language `spel` in the `when` clause.

