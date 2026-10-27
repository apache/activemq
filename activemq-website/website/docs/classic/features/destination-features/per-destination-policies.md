---
title: "Per Destination Policies"
slug: /per-destination-policies
---

Multiple different policies can be applied per destination (queue or topic), or using wildcard notation to apply to a hierarchy of queues or topics, making it possible, therefore, to configure how different regions of the JMS destination space are handled.

The following properties can be applied to either topics and/or queues:

Common Property|Default|Description
---|---|---
`advisoryForConsumed`|`false`|Send an advisory message when a message is consumed by a client.
`advisoryForDelivery`|`false`|Send an advisory message when a message is sent to a client.
`advisoryForFastProducers`|`false`|Send an advisory message if a producer is deemed fast.
`advisoryForSlowConsumers`|`false`|Send an advisory message if a consumer is deemed slow.
`advisoryWhenFull`|`false`|Send an advisory message when a limit (memory, store, temp disk) is full.
`allowTempDestinationStealing`|`false`|Allow other connections besides the connection that created the temporary destination to create consumers on that temporary destination. This is recommended to be `false` and should only be enabled when it's not a concern if other connections can consume from any temporary destination. This may need to be enabled to make certain failover scenaiors or networking bridging use cases work that involve temporary destinations. (Since v6.2.7 and v5.19.8)
`enableAudit`|`true`|When `true` the broker will track duplicate messages. Duplicates can happen for non-persistent messages during failover.
`gcInactiveDestinations`|`false`|Garbage collect inactive destinations.
`inactiveTimoutBeforeGC`|`5000`|The timeout (in ms) after which a destination is considered inactive.
`includeBodyForAdvisory`|`false`|Includes the body of the original message that triggered the advisory as part of the `dataStructure` field in the advisory message (where applicable). Normally the message body is cleared.
`maxBrowsePageSize`|`400`|The maximum number of messages to page in from the store at one time for a browser.
`maxDestinations`|`-1`|(v5.12) If `>= 0`, sets the maximum number of destinations that can be created. This parameter is intended to limit the number of hierarchical destinations that can be created under a wildcard destination.
`maxPageSize`|`200`|The maximum number of messages to page in from the store at one time. Increase this value to improve performance for queue destination's that contain grouped messages that are consumed by multiple concurrent consumers.
`memoryLimit`|`n/a`|The memory limit (in bytes) of the destination's cursor. This memory limit is subordinate to the system level memory limit, as specified by the [`<systemUsage>/<memoryUsage>`](producer-flow-control) attribute. There is no default for this value; it simply acts as a child to the overall broker memory until the broker memory is exhausted. **Note**: when this limit is specified the destination's `cursorMemoryHighWaterMark` will be applied against it and not the `<systemUsage>/><memoryUsage>` memory limit.
`minimumMessageSize`|`1024`|For non-serialized messages (embedded broker) - the assumed size of the message used for memory usage calculation. Serialized messages use the serialized size as the basis for the memory calculation.
`prioritizedMessages`|`false`|Persist message priority information.
`producerFlowControl`|`true`|If `true` the broker will throttle (flow-control) the producer. Throttling is achieved either by withholding the producer's ACK or by raising a `javax.jms.ResourceAllocationException` (that's propagated back to the client) when local resources, e.g., memory and/or storage, have been exhausted. If `false` excess messages will be written to the message store to prevent memory exhaustion. However, when the message store reaches capacity the producer will be throttled until resources are freed.
`slowConsumerStrategy`|`null`|Sets the strategy for handling slow consumers. See [AbortSlowConsumerStrategy.](https://github.com/apache/activemq/blob/master/activemq-broker/src/main/java/org/apache/activemq/broker/region/policy/AbortSlowConsumerStrategy.java)
`storeUsageHighWaterMark`|`100`|The percentage (%) threshold of the `<systemUsage>/<storeUsage>` store limit which when exceeded causes a producer send to block.
`useCache`|`true`|If `true` persistent messages are cached for fast retrieval from store.
`usePrefetchExtension`  |`true`|The prefetch extension is used when a message is delivered but not ACK'ed, such that the broker can dispatch another message, e.g., `prefetch == 0`, the idea being that there will always be prefetch number of messages pending. It also allows a transaction batch to exceed the prefetch value.
`sendDuplicateFromStoreToDLQ` |`false`|(v5.17.0) If `true`, will send a copy of message to DLQ when duplicateFromStore condition is detected. Before v5.17.0, the default behavior was `true`
`sendFailIfNoSpace`|`false`|(v5.16.0) If `true`, will cause a send to fail with a `javax.jms.ResourceAllocationException` when the destination has reached is resource limits (memory or storage)
`sendFailIfNoSpaceAfterTimeout`|`0`|(v5.16.0) If `> 0`, will cause a send to fail with a `javax.jms.ResourceAllocationException` when the destination resource limits (memory or storage) remain exhausted for the configured duration in milliseconds

The following properties can be applied _only_ to a queue:

Queue Only Property|Default|Description
---|---|---
`allConsumersExclusiveByDefault`|`false`|When `true` all consumers will be exclusive. See [Exclusive Consumers](exclusive-consumer)
`cursorMemoryHighWaterMark`|`70`|The percentage (%) threshold applied either to the `<systemUsage>/<memoryUsage>` or the destination's `memoryLimit` (when defined) which when exceeded will cause the destination's cursor to either block or write to disk.
`consumersBeforeDispatchStarts`|`0`|When the first consumer connects, wait for specified number of consumers before message dispatching starts.
`expireMessagesPeriod`|`30000`|The period (in ms) of checks for message expiry on queued messages. A value of `0` will disable expiration checking.
`lazyDispatch`|`false`|Only page in from store the number of messages that can be dispatched at time.
`maxExpirePageSize`|`400`|The maximum number of messages to page in when periodically expiring messages. Messages are paged in according to this setting if the number of messages in memory pending dispatch is less than this value, and there are messages in the store to page in. Messages are expired during this paging step as they are brought into memory from the store. Once the paging process is completed, messages are expired from the list of those that are in memory and pending dispatch, then from the list of those that are in memory but not yet targeted at a subscription.
`optimizedDispatch`|`false`|Don't use a separate thread for dispatching from a Queue.
`persistJMSRedelivered`|`false`|(v 5.10) If true, before a persistent message is dispatched by the broker for the first time, the message is rewritten to reflect the possible delivery. This ensures the message `JMSRedelivered` header is a reliable indication of possible duplicate delivery.
`queuePrefetch`|`n/a`|Sets the prefetch for consumers that are using the default value.
`strictOrderDispatch`|`false`|If `true` queue will not round robin consumers, but it'll use a single one until its prefetch buffer is full.
`timeBeforeDispatchStarts`|`0`|When the first consumer connects, wait for specified time (in ms) before message dispatching starts.
`useConsumerPriority`|`true`|Use the priority of a consumer when dispatching messages from a Queue.

The following properties can be applied _only_ to a topic:

Topic Only Property|Default|Description
---|---|---
`advisoryForDiscardingMessages`|`false`|Send an advisory when a message is discarded from a non durable subscription.
`cursorMemoryHighWaterMark`|`70`|The percentage (%) threshold applied to the `<systemUsage>/<memoryUsage>` which when exceeded will cause the destination's cursor to either block or write to disk.
`alwaysRetroactive`  |`false`|Makes all subscribers retroactive negating the need to modify the clients to enable this feature.
`durableTopicPrefetch`|`n/a`|Sets the prefetch for durable topic consumers that are using the default value.
`expireMessagesPeriod`  |`30000`|The interval (in ms) between message expiration checks on inactive durable subscribers. **Note**: set to `0` to disable message expiration checking.
`topicPrefetch`|`n/a`|Sets the prefetch for topic consumers that are using the default value.

The following are examples of different policies that can be customized on a per destination basis:

*   [Dispatch Policies](dispatch-policies)

An example from the demos [https://git-wip-us.apache.org/repos/asf?p=activemq.git;a=blob;f=assembly/src/release/examples/conf/activemq-demo.xml;hb=HEAD](https://git-wip-us.apache.org/repos/asf?p=activemq.git;a=blob;f=assembly/src/release/examples/conf/activemq-demo.xml;hb=HEAD): 

```
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:amq="http://activemq.apache.org/schema/core"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
                           http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">
 
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>
 
  <broker persistent="false" 
          brokerName="${brokername}" 
          xmlns="http://activemq.apache.org/schema/core">
    
    <destinationPolicy>
      <policyMap>
        <policyEntries>
          <policyEntry topic="FOO.>">
            <dispatchPolicy>
              <roundRobinDispatchPolicy/>
            </dispatchPolicy>
            <subscriptionRecoveryPolicy>
              <lastImageSubscriptionRecoveryPolicy/>
            </subscriptionRecoveryPolicy>
          </policyEntry>
           
          <policyEntry topic="ORDERS.>">
            <dispatchPolicy>
              <strictOrderDispatchPolicy/>
            </dispatchPolicy>
 
            <!-- Recover 1 minute's worth -->
            <subscriptionRecoveryPolicy>
              <timedSubscriptionRecoveryPolicy recoverDuration="60000"/>
            </subscriptionRecoveryPolicy>
          </policyEntry>
     
          <policyEntry topic="PRICES.>">
            <!-- Force pending messages to be discarded for slow consumers -->
            <pendingMessageLimitStrategy>
              <constantPendingMessageLimitStrategy limit="10"/>
            </pendingMessageLimitStrategy>
 
            <!-- 10 seconds worth -->
            <subscriptionRecoveryPolicy>
              <timedSubscriptionRecoveryPolicy recoverDuration="10000"/>
            </subscriptionRecoveryPolicy>
             
          </policyEntry>
          <policyEntry tempTopic="true" advisoryForConsumed="true"/>
          <policyEntry tempQueue="true" advisoryForConsumed="true"/>
        </policyEntries>
      </policyMap>
    </destinationPolicy>
  </broker>
</beans>
```
