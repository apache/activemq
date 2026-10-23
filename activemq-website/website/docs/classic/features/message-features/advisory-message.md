---
title: "Advisory Message"
slug: /advisory-message
---


ActiveMQ supports advisory messages which allows you to watch the system using regular JMS messages. Currently we have advisory messages that support:

*   consumers, producers and connections starting and stopping
*   temporary destinations being created and destroyed
*   messages expiring on topics and queues
*   brokers sending messages to destinations with no consumers.
*   connections starting and stopping

Advisory messages can be thought as some kind of administrative channel where you receive information regarding what is happening on your JMS provider along with what's happening with producers, consumers and destinations. When you look at a broker via [JMX](jmx) you will see the advisory topics prefixed with `ActiveMQ.Advisory.`.

Every Advisory has the message **type** `Advisory` and some predefined message properties:

Property|Type|Description|Version
---|---|---|---
`originBrokerId`|`StringProperty`|The id of the broker where the advisory originated.|5.x
`originBrokerName`|`StringProperty`|The name of the broker where the advisory originated.|5.x
`originBrokerURL`|`StringProperty`|The first URL of the broker where the advisory originated.|5.2

In addition, some messages carry a `Command` object, which carries additional information about the nature of the advisory, e.g., a subscription to each of the destination returns an `ActiveMQMessage`. Specific `DataStructure` objects, e.g.,. `ConsumerInfo`, `ProducerInfo`, `ConnectionInfo` can be retrieved using `ActiveMQMessage.getDataStructure()`.

For example:
```
Destination advisoryDestination = AdvisorySupport.getProducerAdvisoryTopic(destination)
MessageConsumer consumer = session.createConsumer(advisoryDestination);
consumer.setMessageListener(this);

public void onMessage(Message msg){
    if(msg instanceof ActiveMQMessage) {
        try {
             ActiveMQMessage aMsg = (ActiveMQMessage)msg;
             ProducerInfo prod = (ProducerInfo) aMsg.getDataStructure();
        } 
        catch(JMSException e) {
            log.error("Failed to process message: " + msg);
        }
    }
}
```

The following advisory topics are supported
-------------------------------------------

### Client based advisories

> **Tip**
> 
> These are always generated.

Advisory Topics|Description|Properties|Data Structure
---|---|---|---
`ActiveMQ.Advisory.Connection`|Connection start & stop messages.| |`ConnectionInfo`, `RemoveInfo`
`ActiveMQ.Advisory.Producer.Queue`|Producer start & stop messages on a Queue.|`String='producerCount'` - the number of producers|`ProducerInfo`
`ActiveMQ.Advisory.Producer.Topic`|Producer start & stop messages on a Topic.|`String='producerCount'` - the number of producers|`ProducerInfo`
`ActiveMQ.Advisory.Consumer.Queue`|Consumer start & stop messages on a Queue.|`String='consumerCount'` - the number of Consumers|`ConnectionInfo`, `RemoveInfo`
`ActiveMQ.Advisory.Consumer.Topic`|Consumer start & stop messages on a Topic.|`String='consumerCount'` - the number of Consumers|`ConnectionInfo`, `RemoveInfo`

Note that the consumer start/stop advisory messages also have a `consumerCount` header to indicate the number of active consumers on the destination when the advisory message was sent. This means you can use the following selector to be notified when there are no active consumers on a given destination:
```
consumerCount = 0
```

### Destination and Message based advisories

Advisory Topics|Description|Properties|Data Structure|Default|PolicyEntry Property
---|---|---|---|---|---
`ActiveMQ.Advisory.Queue`|Queue create & destroy.|`null`|`DestinationInfo`|`true`|`none`
`ActiveMQ.Advisory.Topic`|Topic create & destroy.|`null`|`DestinationInfo`|`true`|`none`
`ActiveMQ.Advisory.TempQueue`|Temporary Queue create & destroy.|`null`|`DestinationInfo`|`true`|`none`
`ActiveMQ.Advisory.TempTopic`|Temporary Topic create & destroy.|`null`|`DestinationInfo`|`true`|`none`
`ActiveMQ.Advisory.Expired.Queue`|Expired messages on a Queue.|`String='orignalMessageId'` - the expired id|`Message`|`true`|`none`
`ActiveMQ.Advisory.Expired.Topic`|Expired messages on a Topic.|`String='orignalMessageId'` - the expired id|`Message`|`true`|`none`
`ActiveMQ.Advisory.NoConsumer.Queue`|No consumer is available to process messages being sent on a Queue.|`null`|`Message`|`false`|`sendAdvisoryIfNoConsumers`
`ActiveMQ.Advisory.NoConsumer.Topic`|No consumer is available to process messages being sent on a Topic.|`null`|`Message`|`false`|`sendAdvisoryIfNoConsumers`

> **Tip**
> 
> `NoConsumer` topic advisories are sent only for non-persistent messages.

### New advisories in version 5.2

Advisory Topics|Description|Properties|Data Structure|Default|PolicyEntry Property
---|---|---|---|---|---
`ActiveMQ.Advisory.SlowConsumer.Queue`|Slow Queue Consumer.|`String='consumerId'` - the consumer id|`ConsumerInfo`|`false`|`advisoryForSlowConsumers`
`ActiveMQ.Advisory.SlowConsumer.Topic`|Slow Topic Consumer.|`String='consumerId'` - the consumer id|`ConsumerInfo`|`false`|`advisoryForSlowConsumers`
`ActiveMQ.Advisory.FastProducer.Queue`|Fast Queue producer.|`String='producerId'` - the producer id|`ProducerInfo`|`false`|`advisoryForFastProducers`
`ActiveMQ.Advisory.FastProducer.Topic`|Fast Topic producer.|`String='consumerId'`' - the producer id|`ProducerInfo`|`false`|`advisoryForFastProducers`
`ActiveMQ.Advisory.MessageDiscarded.Queue`|Message discarded.|`String='orignalMessageId'` - the discarded id|`Message`|`false`|`advisoryForDiscardingMessages`
`ActiveMQ.Advisory.MessageDiscarded.Topic`|Message discarded.|`String='orignalMessageId'` - the discarded id|`Message`|`false`|`advisoryForDiscardingMessages`
`ActiveMQ.Advisory.MessageDelivered.Queue`|Message delivered to the broker.|`String='orignalMessageId'` - the delivered id|`Message`|`false`|`advisoryForDelivery`
`ActiveMQ.Advisory.MessageDelivered.Topic`|Message delivered to the broker.|`String='orignalMessageId'` - the delivered id|`Message`|`false`|`advisoryForDelivery`
`ActiveMQ.Advisory.MessageConsumed.Queue`|Message consumed by a client.|`String='orignalMessageId'` - the delivered id|`Message`|`false`|`advisoryForConsumed`
`ActiveMQ.Advisory.MessageConsumed.Topic`|Message consumed by a client.|`String='orignalMessageId'` - the delivered id|`Message`|`false`|`advisoryForConsumed`
`ActiveMQ.Advisory.FULL`|A Usage resource is at its limit.|`String='usageName'` - the name of Usage resource|`null`|`false`|`advisoryWhenFull`
`ActiveMQ.Advisory.MasterBroker`|A broker is now the master in a master/slave configuration.|`null`|`null`|`true`|`none`

### New Advisories in 5.4

Advisory Topics|Description|Properties|Data Structure|Default|PolicyEntry Property
---|---|---|---|---|---
`ActiveMQ.Advisory.MessageDLQd.Queue`|Message sent to DLQ.|`String='orignalMessageId'` - the delivered id|`Message`|Always on|`advisoryForConsumed`
`ActiveMQ.Advisory.MessageDLQd.Topic`|Message sent to DLQ.|`String='orignalMessageId'` - the delivered id|`Message`|Always on|`advisoryForConsumed`

### Network bridge advisories

Starting with ActiveMQ version 5.5 you can watch advisory topics for events related to the status of network bridges. You can get advisory messages when the network bridge is started or stopped.

Advisory Topics|Description|Properties|Data Structure|Default
---|---|---|---
`ActiveMQ.Advisory.NetworkBridge`|Network bridge being stopped or started.|`Boolean="started"` - `true` if bridge is started, `false` if it is stopped. `Boolean="createdByDuplex"` - `true` if the bridge is created by remote network connector.|`BrokerInfo` - provides data of the remote broker|Always on

### Enabling Advisories Disabled by Default

The advisories that are not turned on by default (see the last column) can be enabled on a `PolicyEntry` in the ActiveMQ Broker Configuration, e.g., to enable a message consumed advisory you can configure the following:
```
<destinationPolicy>
    <policyMap>
       <policyEntries> 
           <policyEntry topic=">" advisoryForConsumed="true"/>
       </policyEntries>
    </policyMap>
</destinationPolicy>
```

> Hint
> 
> The `>` character matches all topics - you can use wild-card matches for setting a destination policy - see [Wildcards](wildcards)

### Disabling Advisory Messages

The use of advisory messages incurs a small overhead in terms of memory and connection resources that is related to the number of destinations in your system. In some cases it can make sense to disable all advisories.

Advisories need to be disabled both on the Broker, via XML Configuration
```
<broker advisorySupport="false">
```
or from java code:
```
BrokerService broker = new BrokerService();
broker.setAdvisorySupport(false);
// ...
broker.start();
```
_and_ on your `ActiveMQConnectionFactory` (because a subscription to an advisory topic will auto create it) via the `brokerUrl`:
```
tcp://localhost:61616?jms.watchTopicAdvisories=false
```
or via java code using the `watchTopicAdvisories` attribute on the `ActiveMQConnectionFactory`.
```
ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
factory.setWatchTopicAdvisories(false);
```

> **Warning**
> 
> Advisory messages are required for [dynamic network broker topologies](networks-of-brokers) as `NetworkConnectors` subscribe to advisory messages. In the absence of advisories, a network must be statically configured.

### Using the Destinations

All of the above destinations are really prefixes which are appended with important information (like the actual topic or queue, the client ID, producer ID, consumer ID etc). This allows you to reuse the power of publish/subscribe, [Wildcards](wildcards) and [Selectors](selectors) to filter the advisory messages as you see fit.

For example if you want to subscribe to expired messages on a topic `FOO.BAR` you could subscribe to `ActiveMQ.Advisory.Expired.Topic.FOO.BAR`. To subscribe to all messages of a certain kind of advisory just append `.>` to the topic, e.g., to subscribe to all the consumers starting and stopping to topics and queues subscribe to `ActiveMQ.Advisory.Consumer..>`.

### Helper Methods

Methods to get the advisory destination objects are available in [AdvisorySupport](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/advisory/AdvisorySupport.html) through the following methods.

* AdvisorySupport.getConsumerAdvisoryTopic()
* AdvisorySupport.getProducerAdvisoryTopic()
* AdvisorySupport.getExpiredTopicMessageAdvisoryTopic()
* AdvisorySupport.getExpiredQueueMessageAdvisoryTopic()
* AdvisorySupport.getNoTopicConsumersAdvisoryTopic()
* AdvisorySupport.getNoQueueConsumersAdvisoryTopic()
* AdvisorySupport.getDestinationAdvisoryTopic()
* AdvisorySupport.getExpiredQueueMessageAdvisoryTopic()
* AdvisorySupport.getExpiredTopicMessageAdvisoryTopic()
* AdvisorySupport.getNoQueueConsumersAdvisoryTopic()
* AdvisorySupport.getNoTopicConsumersAdvisoryTopic()

// Version 5.2 on:

* AdvisorySupport.getSlowConsumerAdvisoryTopic()
* AdvisorySupport.getFastProducerAdvisoryTopic()
* AdvisorySupport.getMessageDiscardedAdvisoryTopic()
* AdvisorySupport.getMessageDeliveredAdvisoryTopic()
* AdvisorySupport.getMessageConsumedAdvisoryTopic()
* AdvisorySupport.getMasterBrokerAdvisoryTopic()
* AdvisorySupport.getFullAdvisoryTopic()

Some helper classes to deal with advisory messages are available in the [advisories](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/advisory/package-summary.html) package.

