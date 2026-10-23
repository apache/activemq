---
title: "Activation Spec Properties"
slug: /activation-spec-properties
---


An Activation Spec is used to configure the message delivery to an MDB. The ejb-jar.xml deployment descriptor needs to include a <activation-config> element inside the <message-driven> element like:
```
<activation-config>
    <activation-config-property>
       <activation-config-property-name>destination</activation-config-property-name>
       <activation-config-property-value>queue.testQueue</activation-config-property-value>
    </activation-config-property>
    <activation-config-property>
       <activation-config-property-name>destinationType</activation-config-property-name>
       <activation-config-property-value>javax.jms.Queue</activation-config-property-value>
    </activation-config-property>
</activation-config>
```
Here, the value for destination is the physical name of the desired destination. The value for destinationType is the class name that defines the type of destination. It should be javax.jms.Queue or javax.jms.Topic.  
 
The Activation Spec properties that can be configured are:

Property Name|Required|Default Value|Description
---|---|---|---
acknowledgeMode|no|Auto-acknowledge|The JMS Acknowledgement mode to use. Valid values are: Auto-acknowledge or Dups-ok-acknowledge
clientId|no|set in resource adapter|The JMS Client ID to use (only really required for durable topics)
destinationType|yes|null|The type of destination; a queue or topic
destination|yes|null|The destination name (queue or topic name)
enableBatch|no|false|Used to enable transaction batching for increased performance
maxMessagesPerBatch|no|10|The number of messages per transaction batch
maxMessagesPerSessions|no|10|This is actually the prefetch size for the subscription. (Yes, badly named).
maxSessions|no|10|The maximum number of concurrent sessions to use
messageSelector|no|null|The JMS [Message Selector](selectors) to use on the subscription to perform content based routing filtering the messages
noLocal|no|false|Only required for topic subscriptions; indicates if locally published messages should be included in the subscription or not
password|no|set in resource adapter|The password for the JMS connection
subscriptionDurability|no|NonDurable|Whether or not a durable (topic) subscription is required. Valid values are: Durable or NonDurable
subscriptionName|no|null|The name of the durable subscriber. Only used for durable topics and combined with the clientID to uniquely identify the durable topic subscription
userName|no|set in resource adapter|The user for the JMS connection
useRAManagedTransaction|no|false|Typically, a resource adapter delivers messages to an endpoint which is managed by a container. Normally, this container likes to be the one that wants to control the transaction that the inbound message is being delivered on. But sometimes, you want to deliver to a simpler container system that will not be controlling the inbound transaction. In these cases, if you set useRAManagedTransaction to true, the resource adapter will commit the transaction if no exception was generated from the MessageListener and rollback if an exception is thrown.
initialRedeliveryDelay|no|1000|The delay before redeliveries start. Also configurable on the ResourceAdapter
maximumRedeliveries|no|5|The maximum number of redeliveries or -1 for no maximum. Also configurable on the ResourceAdapter
redeliveryBackOffMultiplier|no|5|The multiplier to use if exponential back off is enabled. Also configurable on the ResourceAdapter
redeliveryUseExponentialBackOff|no|false|To enable exponential backoff. Also configurable on the ResourceAdapter
useJndi|no|false|when true, use destination as a jndi name

##### Maximising Throughput of MDBs

If you want to maximise throughput of MDBs you should really set the **maxSessions** to something fairly large to increase the concurrency. Then set **maxMessagesPerSessions** to something big (say) 1000.

This assumes you have large numbers of messages available (say more than **maxSessions** \* **maxMessagesPerSession**). Otherwise the [prefetch](what-is-the-prefetch-limit-for) will end up [starving other consumers](i-do-not-receive-messages-in-my-second-consumer).

So if you don't have that many messages available, but maybe they take a while to process then you might want to set a lower value of **maxMessagesPerSessions**

