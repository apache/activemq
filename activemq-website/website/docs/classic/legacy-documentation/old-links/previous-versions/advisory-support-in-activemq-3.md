---
title: "Advisory Support in ActiveMQ 3"
slug: /advisory-support-in-activemq-3
---

For users of Apache ActiveMQ 4.0 or later please see [Advisory Message](advisory-message). In previous 3.x versions the following is supported:-

The destination string constants are available in [ActiveMQDestination](http://activemq.codehaus.org/maven/apidocs/org/activemq/message/ActiveMQDestination.html)

```
ActiveMQDestination.CONSUMER_ADVISORY_PREFIX
ActiveMQDestination.PRODUCER_ADVISORY_PREFIX
ActiveMQDestination.CONNECTION_ADVISORY_PREFIX
ActiveMQDestination.TEMP_DESTINATION_ADVISORY_PREFIX
```

A subscription to each of the destination returns an ObjectMessage with a specific object: [ConsumerInfo](http://activemq.codehaus.org/maven/apidocs/org/activemq/message/ConsumerInfo.html), [ProducerInfo](http://activemq.codehaus.org/maven/apidocs/org/activemq/message/ProducerInfo.html),[ConnectionInfo](http://activemq.codehaus.org/maven/apidocs/org/activemq/message/ConnectionInfo.html), [TempDestinationAdvisoryEvent](http://activemq.codehaus.org/maven/apidocs/org/activemq/advisories/TempDestinationAdvisoryEvent.html).

For example:

```
... 
String advisoryName = ActiveMQDestination.PRODUCER_ADVISORY_PREFIX + destination.getPhysicalName(); 
Destination advisoryDestination = ActiveMQDestination.createDestination(destination.getDestinationType(), advisoryName); 
MessageConsumer consumer = session.createConsumer(advisoryDestination); 
consumer.setMessageListener(this); 
.... 

public void onMessage(Message msg) {
   if (msg instanceof ObjectMessage) {
      try {
         ProducerInfo info = (ProducerInfo)((ObjectMessage)msg).getObject();
      } catch (JMSException e) {
         log.error("Failed to process message: " + msg); 
      } 
   }
}
```
