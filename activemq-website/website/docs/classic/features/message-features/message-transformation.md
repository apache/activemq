---
title: "Message Transformation"
slug: /message-transformation
---


Message Transformation
----------------------

It is sometimes useful to transform a message inside the JMS provider. For example you may have an application that has been live for some time that uses ObjectMessage messages, but that you wish to convert to use XML payloads (to protect yourself from serialization issues).

So ActiveMQ in 4.2 onwards comes with a pluggable strategy called the [MessageTransformer](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/MessageTransformer.html) interface. This allows you to

*   enrich or transform a message before it is sent to the message bus within the JMS provider
*   enrich or transform a message as it is received from the message bus but before it is dispatched to the consumer

So this allows you to turn an ObjectMessage into a TextMessage containing XML using some kind of XML marshalling technology like [XStream](http://xstream.codehaus.org/) or [JAXB2](http://java.sun.com/webservices/jaxb/).

For example there is the **XStreamMessageTransformer** which when it is configured on a ConnectionFactory will cause all ObjectMessage instances to be silently transformed to and from TextMessage instances on the wire. This can be very useful if you have non-Java clients wishing to communicate with your Java messages.

### Using a MessageTransformer

To use a MessageTransformer you can install it on one of the following classes via the **setTransformer()** method

*   [ActiveMQConnectionFactory](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQConnection.html)
*   [ActiveMQConnection](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQConnection.html)
*   [ActiveMQSession](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQSession.html)
*   [ActiveMQMessageConsumer](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQMessageConsumer.html)
*   [ActiveMQMessageProducer](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQMessageProducer.html)

The transformer is inherited on child objects; so you can install a particular transformer on a connection factory and it will be inherited on all connections, sessions, producers, consumers

