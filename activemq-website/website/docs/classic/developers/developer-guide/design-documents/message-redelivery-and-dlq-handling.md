---
title: "Message Redelivery and DLQ Handling"
slug: /message-redelivery-and-dlq-handling
---


### Overview

Messages are redelivered to a client when **any** of the following occurs:

1.  A transacted session is used and `rollback()` is called.
2.  A transacted session is closed before `commit()` is called.
3.  A session is using `CLIENT_ACKNOWLEDGE` and `Session.recover()` is called.
4.  A client connection times out (perhaps the code being executed takes longer than the configured time-out period).

The broker transmits the default delivery policy that he prefers to a client connection in his `BrokerInfo` command packet. But the client can override the policy settings by using the `ActiveMQConnection.getRedeliveryPolicy()` method:
```
RedeliveryPolicy policy = connection.getRedeliveryPolicy();
policy.setInitialRedeliveryDelay(500);
policy.setBackOffMultiplier(2);
policy.setUseExponentialBackOff(true);
policy.setMaximumRedeliveries(2);
```
Once a message's redelivery attempts exceeds the `maximumRedeliveries` configured for the [Redelivery Policy](redelivery-policy), a "Poison ACK" is sent back to the broker letting him know that the message was considered a poison pill. The Broker then takes the message and sends it to a Dead Letter Queue so that it can be analyzed later on.

The default Dead Letter Queue in ActiveMQ is called `ActiveMQ.DLQ`; all un-deliverable messages will get sent to this queue and this can be difficult to manage. So, you can set an `individualDeadLetterStrategy` in the destination policy map of the `activemq.xml` configuration file, which allows you to specify a specific dead letter queue prefix for a given queue or topic. You can apply this strategy using wild card if you like so that all queues get their own dead-letter queue, as is shown in the example below.
```
<broker>
  
  <destinationPolicy>
    <policyMap>
      <policyEntries>
        <!-- Set the following policy on all queues using the '>' wildcard -->
        <policyEntry queue=">">
          <deadLetterStrategy>
            <!--
              Use the prefix 'DLQ.' for the destination name, and make
              the DLQ a queue rather than a topic
            -->
            <individualDeadLetterStrategy queuePrefix="DLQ." useQueueForQueueMessages="true"/>
          </deadLetterStrategy>
        </policyEntry>
      </policyEntries>
    </policyMap>
  </destinationPolicy>
  
</broker>
```
See the [Redelivery Policy](redelivery-policy) section for some more detail on the policy options.

### Automatically Discard Expired Messages

Some folks simply need expired messages to be discarded instead of sent to the DLQ i.e., skip the DLQ entirely. This simplifies the management of the DLQ so that you're not sifting through loads of expired messages to find messages with real problems. To tell ActiveMQ to just discard expired messages, configure the `processExpired` property to false on a dead letter strategy:
```
<broker>
  
  <destinationPolicy>
   <policyMap>
     <policyEntries>
       <!-- Set the following policy on all queues using the '>' wildcard -->
       <policyEntry queue=">">
         <!-- 
           Tell the dead letter strategy not to process expired messages
           so that they will just be discarded instead of being sent to
           the DLQ 
         -->
         <deadLetterStrategy>
           <sharedDeadLetterStrategy processExpired="false" />
         </deadLetterStrategy>
       </policyEntry>
     </policyEntries>
   </policyMap>
  </destinationPolicy>
  
</broker>
```

### Place Non-Persistent Messages Onto The Dead-Letter Queue

By default, ActiveMQ will not place undeliverable _non-persistent_ messages on the dead-letter queue. The rationale for this behavior is that if the application doesn't care enough to make the message persistent, then there is little or no value in recording that the message was undeliverable. If you do want to place non-persistent messages on the dead-letter queue, then you should set `processNonPersistent="true"` on the dead-letter strategy.
```
<broker>
  
  <destinationPolicy>
   <policyMap>
     <policyEntries>
       <!-- Set the following policy on all queues using the '>' wildcard -->
       <policyEntry queue=">">
         <!-- 
           Tell the dead letter strategy to also place non-persisted messages 
           onto the dead-letter queue if they can't be delivered.
         -->
         <deadLetterStrategy>
           <sharedDeadLetterStrategy processNonPersistent="true" />
         </deadLetterStrategy>
       </policyEntry>
     </policyEntries>
   </policyMap>
  </destinationPolicy>
  
</broker>
```

### Setting Expiration on Messages in the DLQ

By default, ActiveMQ will **_never_** expire messages sent to the DLQ. However, from ActiveMQ 5.12 the `deadLetterStrategy` supports an `expiration` attribute whose value is given in milliseconds.

> Be selective in how this is applied. In particular do not apply expiration to your DLQ destinations by setting expiration on a default or inclusive wildcard policy entry.
> 
> If a DLQ entry expires and forwards to the same or another DLQ with expiry, you will introduce a loop that can be problematic if the strategy audit is disabled or it's sliding window is exceeded.

```
<broker>
  
  <destinationPolicy>
   <policyMap>
     <policyEntries>
       <policyEntry queue="QueueWhereItIsOkToExpireDLQEntries">
         <deadLetterStrategy>
           <.... expiration="300000"/>
         </deadLetterStrategy>
       </policyEntry>
     </policyEntries>
   </policyMap>
  </destinationPolicy>
  
</broker>
```

### Message audit

The dead letter strategy has an message audit that is enabled by default. This prevents duplicate messages from being added to the configured DLQ. From 5.15.0, the limits of the audit can configured via the

`maxProducersToAudit` and `maxAuditDepth` attributes. The audit can be disabled using `enableAudit="false"`

### The Discarding DLQ Plugin

> From ActiveMQ 5.9 - a destination `policyEntry` supports a `deadLetterStrategy` of discarding:
> ```
> <deadLetterStrategy>
>   <discarding/>
> </deadLetterStrategy>
> ```
> This does the same thing as the plugin but on a per destination basis. The matching based on regular expressions of the plugin is a bit more powerful than destination matching so the plugin may still be useful in some cases.

A very simple yet very useful plugin to the broker. This plugin allows the configuration of queues and topics, all or matched based on [Java SE regular expressions](http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html), to drop messages that have been sent to the DLQ. This is extremely useful when using [constant pending message limit strategy](slow-consumer-handling) or the other eviction rules, but you don't want to incur the overhead of yet another consumer to clear the DLQ.

Below is an example of a basic configuration to drop everything:
```
<beans> 
  <broker>
    <plugins>
      <discardingDLQBrokerPlugin dropAll="true" dropTemporaryTopics="true" dropTemporaryQueues="true"/>
    </plugins>
  </broker>
</beans>
```
Below is a slightly more complex example:
```
<beans> 
  <broker>
    <plugins>
      <discardingDLQBrokerPlugin dropOnly="MY.EXAMPLE.TOPIC.29 MY.EXAMPLE.QUEUE.87" reportInterval="1000"/>
    </plugins>
  </broker>
</beans>
```
*   Notice that destination names are space delimited.
*   The `reportInterval` property is used to denote how frequently do we output how many messages we have dropped - use `0` to disable.

Below is an even more complex example:
```
<beans> 
  <broker>
    <plugins>
      <discardingDLQBrokerPlugin dropOnly="MY.EXAMPLE.TOPIC.[0-9]{3} MY.EXAMPLE.QUEUE.[0-9]{3}" reportInterval="3000"/>
    </plugins>
  </broker>
</beans>
```
*   Notice that the destination names use regular expressions. These match the number `000..999` at the end of each destination name.

For more information, see the source code for the [DiscardingDLQBrokerPlugin](https://github.com/apache/activemq/tree/main/activemq-broker/src/main/java/org/apache/activemq/plugin/DiscardingDLQBrokerPlugin.java) and the [DiscardingDLQBroker](https://github.com/apache/activemq/tree/main/activemq-broker/src/main/java/org/apache/activemq/plugin/DiscardingDLQBroker.java)

### Broker Redelivery (v5.7)

Typically a consumer handles redelivery so that it can maintain message order while a message appears as inflight on the broker. This means that redelivery is limited to a single consumer unless that consumer terminates. In this way the broker is unaware of redelivery. With broker redelivery, it is possible to have the broker redeliver a message after a delay using a resend. This is implemented by a broker plugin that handles dead letter processing by redelivery via the scheduler. This is useful when total message order is not important and where through put and load distribution among consumers is. With broker redelivery, messages that fail delivery to a given consumer can get immediately re-dispatched.

  
The feature is enabled via XML configuration as follows:
```
<broker schedulerSupport="true">
        
        <plugins>
            <redeliveryPlugin fallbackToDeadLetter="true" 
                              sendToDlqIfMaxRetriesExceeded="true">
                <redeliveryPolicyMap>
                    <redeliveryPolicyMap>
                        <redeliveryPolicyEntries>
                            <!-- a destination specific policy -->
                            <redeliveryPolicy queue="SpecialQueue" 
                                              maximumRedeliveries="4" 
                                              redeliveryDelay="10000"/>
                        </redeliveryPolicyEntries>
                        
                        <defaultEntry>
                            <!-- the fallback policy for all other destinations -->
                            <redeliveryPolicy maximumRedeliveries="4" 
                                              initialRedeliveryDelay="5000"
                                              redeliveryDelay="10000"/>
                        </defaultEntry>
                    </redeliveryPolicyMap>
                </redeliveryPolicyMap>
            </redeliveryPlugin>
        </plugins>
        
</broker> 
```
The familiar [Redelivery Policy](redelivery-policy) has been extended to take a matching destination. `fallbackToDeadLetter`controls the action when there is no matching redeliver policy for a destination. Defaults to `true` so regular DLQ processing ensues. `sendToDlqIfMaxRetriesExceeded` controls the action when the retry limit is exceeded. Defaults to true so regular DLQ processing ensues. When `false`, the message is dropped.

> ActiveMQ's `schedulerSupport` must be enabled for this feature to work.

