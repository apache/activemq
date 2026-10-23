---
title: "Slow Consumer Handling"
slug: /slow-consumer-handling
---


[Slow Consumers](slow-consumers) can cause problems on non-durable topics since they can force the broker to keep old messages in RAM which once it fills up, forces the broker to slow down producers, causing the fast consumers to be slowed down. One option we could implement in the future is spooling to disk - but then spooling to disk could slow down the fast consumers too.

Currently we have a strategy that lets you configure the maximum number of matched messages the broker will keep around for a consumer in addition to its prefetch buffer. Once this maximum is reached, as new messages come in, older messages are discarded. This allows you to keep the RAM for current messages and keep sending messages to a slow consumer but to discard old messages.

Pending Message Limit Strategy
------------------------------

You can configure the `PendingMessageLimitStrategy` implementation class on the destination map so that different regions of your topic namespace can have different strategies for dealing with slow consumers. For example you may want to use this strategy for prices which are very high volume but for orders and trades which are lower volume you might not wish to discard old messages.

The strategy calculates the maximum number of pending messages to be kept in RAM for a consumer (above its prefetch size). A value of zero means keep no messages around other than the prefetch amount. A value greater than zero will keep up to that amount of messages around, discarding the older messages as new messages come in. A value of `-1` disables the discarding of messages.

There are currently two different implementations of the strategy:

*   `ConstantPendingMessageLimitStrategy`
*   `PrefetchRatePendingMessageLimitStrategy`

### ConstantPendingMessageLimitStrategy

This strategy uses a constant limit for all consumers (above their prefetch size).

Example:

```
<constantPendingMessageLimitStrategy limit="50"/>
```

### PrefetchRatePendingMessageLimitStrategy

This strategy calculates the maximum number of pending messages using a multiplier of the consumers prefetch size. So you could for example keep around 2.5 times the prefetch count for each consumer.

```
<prefetchRatePendingMessageLimitStrategy multiplier="2.5"/>
```

### Using the Prefetch Policy to Configure the Limit

The JMS Client has a [prefetch policy](what-is-the-prefetch-limit-for) you can use to configure the various prefetch limits for persistent and non persistent queues and topics. The prefetch policy also allows you to specify the `maximumPendingMessageLimit` on a per connection/consumer basis. One minor difference with configuring this value; to simplify operation with non-JMS clients such as with [OpenWire](openwire) the value of zero is ignored; so the lowest value you can configure is `1`.

### Configuring the Eviction Policy

We have a `MessageEvictionStrategy` which is used to decide which message should be evicted on a slow consumer. The default implementation is:

```
<oldestMessageEvictionStrategy/>
```

By default, ActiveMQ will frist remove any expired messages from the slow-consumer pending buffer before evicting non-expired messages.

If your messages do not use TTL or you do not want this behavior you can disable it with `expiryCheckEnabled="false"`:

```
<oldestMessageEvictionStrategy expiryCheckEnabled="false"/>
```

Messages will still be subject to expiry checks on the client. For the full client/broker expiry flow and the clock synchronization caveat, see [Message Expiry](message-expiry). The default is `expiryCheckEnabled="true"`.

However, you can write your own to use some application specific way of choosing messages for eviction. For example, if you are sending market data price updates you may wish to find an older price value, which might not be the oldest message.

Example:

```
<uniquePropertyMessageEvictionStrategy propertyName="STOCK"/>

```
where `propertyName` is the JMS message property that specifies the price.

Another option could be to use the oldest message with the lowest priority message. Therefore if you have some high priority messages, evict the lower priority messages first even if they are newer.

```
<oldestMessageWithLowestPriorityEvictionStrategy/>
```

Example
-------

The following example shows an ActiveMQ broker configuration file. Notice that for topics in the `PRICES.>` wildcard range the `pendingMessageLimitStrategy` property is set to only keep around `10` messages for each consumer above their prefetch buffer size.

```
<beans xmlns="http://www.springframework.org/schema/beans" 
       xmlns:amq="http://activemq.apache.org/schema/core" 
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
       xsi:schemaLocation="http://www.springframework.org/schema/beans 
                           http://www.springframework.org/schema/beans/spring-beans.xsd 
                           http://activemq.apache.org/schema/core  
                           http://activemq.apache.org/schema/core/activemq-core.xsd"> 
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/> 
  <broker xmlns="http://activemq.apache.org/schema/core" persistent="false" brokerName="${brokername}"> 
    <!-- lets define the dispatch policy --> 
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
            <!-- 1 minutes worth --> 
            <subscriptionRecoveryPolicy>
              <timedSubscriptionRecoveryPolicy recoverDuration="60000"/>
            </subscriptionRecoveryPolicy> 
          </policyEntry> 
          <policyEntry topic="PRICES.>"> 
          <!-- lets force old messages to be discarded for slow consumers --> 
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

Usage Tips
----------

It is advisable that if you know a particular consumer is going to be slow then set its prefetch size to something smaller than the fast consumers!

For example, if you know a particular server is quite slow and you have very high message rates _and_ you have some very fast consumers then you might want to enable this feature and set the prefetch on the slow servers to be a _little_ lower than on the fast servers.

### Monitoring the Status of Slow Consumers

You can also use a [JMX](jmx) Console to view the statistics of the active subscriptions. This allows you to view the following statistics on a `TopicSubscriptionViewMBean`:

Statistic|Definition
---|---
`discarded`|The count of how many messages have been discarded during the lifetime of the subscription due to it being a slow consumer
`matched`|The current number of messages matched and to be dispatched to the subscription as soon as some capacity is available in the prefetch buffer. So a non-zero value implies that the prefetch buffer is full for this subscription
