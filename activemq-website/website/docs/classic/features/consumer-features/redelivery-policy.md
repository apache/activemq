---
title: "Redelivery Policy"
slug: /redelivery-policy
---


Redelivery Policy
-----------------

Detail on when messages are redelivered to a client can be found in the [Message Redelivery and DLQ Handling](message-redelivery-and-dlq-handling) section. You can configure the [RedeliveryPolicy](http://svn.apache.org/viewvc/activemq/trunk/activemq-client/src/main/java/org/apache/activemq/RedeliveryPolicy.java?view=markup) on your [ActiveMQConnectionFactory](http://svn.apache.org/viewvc/activemq/trunk/activemq-client/src/main/java/org/apache/activemq/ActiveMQConnectionFactory.java?view=markup) or [ActiveMQConnection](http://svn.apache.org/viewvc/activemq/trunk/activemq-client/src/main/java/org/apache/activemq/ActiveMQConnection.java?view=markup) to customize exactly how you want the redelivery to work.

You can use Java code, Spring or the [Connection Configuration URI](connection-configuration-uri) to customize this.

### Available Properties

Property|Default Value|Description
---|---|---
`backOffMultiplier`|`5`|The back-off multiplier.
`collisionAvoidanceFactor`|`0.15`|The percentage of range of collision avoidance if enabled.
`initialRedeliveryDelay`|`1000L`|The initial redelivery delay in milliseconds.
`maximumRedeliveries`|`6`|Sets the maximum number of times a message will be redelivered before it is considered a **poisoned pill** and returned to the broker so it can go to a Dead Letter Queue. Set to `-1` for unlimited redeliveries.
`maximumRedeliveryDelay`|`-1`|Sets the maximum delivery delay that will be applied if the `useExponentialBackOff` option is set. (use value `-1` to define that no maximum be applied) (v5.5).
`redeliveryDelay`|`1000L`|The delivery delay if `initialRedeliveryDelay=0` (v5.4).
`useCollisionAvoidance`|`false`|Should the redelivery policy use collision avoidance.
`useExponentialBackOff`|`false`|Should exponential back-off be used, i.e., to exponentially increase the timeout.

RedeliveryPolicy per Destination
--------------------------------

As of ActiveMQ v5.7.0 you can now configure a [RedeliveryPolicy](http://svn.apache.org/viewvc/activemq/trunk/activemq-client/src/main/java/org/apache/activemq/RedeliveryPolicy.java?view=markup) on a per-destination bases. The `ActiveMQConnection` factory class now exposes a [RedeliveryPolicyMap](http://svn.apache.org/viewvc/activemq/trunk/activemq-client/src/main/java/org/apache/activemq/broker/region/policy/RedeliveryPolicyMap.java?view=markup) property that allows to assign a RedeliveryPolicy using named destinations or using destination wildcards. The code snipped below shows how to configure a different [RedeliveryPolicy](http://svn.apache.org/viewvc/activemq/trunk/activemq-client/src/main/java/org/apache/activemq/RedeliveryPolicy.java?view=markup) for Topics and Queues.
```
ActiveMQConnection connection ...  // Create a connection

RedeliveryPolicy queuePolicy = new RedeliveryPolicy();
queuePolicy.setInitialRedeliveryDelay(0);
queuePolicy.setRedeliveryDelay(1000);
queuePolicy.setUseExponentialBackOff(false);
queuePolicy.setMaximumRedeliveries(2);

RedeliveryPolicy topicPolicy = new RedeliveryPolicy();
topicPolicy.setInitialRedeliveryDelay(0);
topicPolicy.setRedeliveryDelay(1000);
topicPolicy.setUseExponentialBackOff(false);
topicPolicy.setMaximumRedeliveries(3);

// Receive a message with the JMS API
RedeliveryPolicyMap map = connection.getRedeliveryPolicyMap();
map.put(new ActiveMQTopic(">"), topicPolicy);
map.put(new ActiveMQQueue(">"), queuePolicy);
```