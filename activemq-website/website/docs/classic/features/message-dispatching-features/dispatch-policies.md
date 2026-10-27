---
title: "Dispatch Policies"
slug: /dispatch-policies
---

Dispatch Policies
=================

Dispatch policies for queues
----------------------------

Plug-able dispatch policies only apply to topics. For Queues, dispatch is more static, you can choose round robin (the default) or strict order. Before discussing dispatch policies its worth first understanding [the purpose of the prefetch value](what-is-the-prefetch-limit-for).

The out of the box configuration of ActiveMQ is designed for high performance and high throughput messaging where there are lots of messages that need to be dispatched to consumers as quickly as possible. So the default prefetch values are fairly large and the default dispatch policy will try and fill the prefetch buffers as quickly as possible.

However with messaging there are many use cases and sometimes the default configuration is not ideal to your use case; when you send a small number of messages, they tend to all go to one consumer unless you've lots of messages. If you have a large number of consumers and a relatively high [prefetch value](what-is-the-prefetch-limit-for) and you have a small number of messages that each message takes quite a while to process then the default dispatch policy might result in increasing the amount of time it takes to process all the messages (since the load balancing is not fair for small numbers of messages).

For queues, you can define whether the dispatch will occur in a round-robin fashion (default behaviour) or if one consumer's prefetch buffer will be exhausted before the dispatch process selects the next consumer along (strictOrderDispatch).

The latter behaviour is enabled by setting the `strictOrderDispatch` attribute on the `<policyEntry />` element. E.g.:
```
<policyEntry queue=">" strictOrderDispatch="false" />
```
Consumer priorities are observed, so if you have several consumers with different [priorities](consumer-priority), the one with the highest priority will be flooded first until it can take no more, then the next one along, etc.

From version 5.14.0 - the strictOrderDispatch=true option will ensure strict order for redispatched messages when there is a single consumer. 

Dispatch policies for Topics
----------------------------

There are more options for topics because the dispatch policy is plug-able. Any implementation of org.apache.activemq.broker.region.policy.DispatchPolicy will work.

The default `org.apache.activemq.broker.region.policy.SimpleDispatchPolicy` does what one would expect and delivers messages to all subscribers. An example of a more advanced implementation is the `org.apache.activemq.broker.region.policy.PriorityNetworkDispatchPolicy` which will only dispatch to the highest priority network consumer. This is useful in a loop network topology where there is more than route to a consumer.

Here is an [example of destination policy configuration](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/org/apache/activemq/xbean/activemq-policy.xml).

```
<destinationPolicy>
   <policyMap>
      <policyEntries>
         <policyEntry topic="FOO.>">
            <dispatchPolicy>
               <roundRobinDispatchPolicy />
            </dispatchPolicy>
            <subscriptionRecoveryPolicy>
               <lastImageSubscriptionRecoveryPolicy />
            </subscriptionRecoveryPolicy>
         </policyEntry>
         <policyEntry topic="ORDERS.>">
            <dispatchPolicy>
               <strictOrderDispatchPolicy />
            </dispatchPolicy>
            <!-- 1 minutes worth -->
            <subscriptionRecoveryPolicy>
               <timedSubscriptionRecoveryPolicy recoverDuration="60000" /> 
            </subscriptionRecoveryPolicy>
         </policyEntry>
         <policyEntry topic="PRICES.>">
            <!-- lets force old messages to be discarded for slow consumers --> 
            <pendingMessageLimitStrategy>
               <constantPendingMessageLimitStrategy limit="10"/>
            </pendingMessageLimitStrategy>
            <!-- 10 seconds worth -->
            <subscriptionRecoveryPolicy>
               <timedSubscriptionRecoveryPolicy recoverDuration="10000" />
            </subscriptionRecoveryPolicy>
         </policyEntry>
         <policyEntry tempTopic="true" advisoryForConsumed="true" />
         <policyEntry tempQueue="true" advisoryForConsumed="true" />
      </policyEntries>
   </policyMap>
</destinationPolicy>
```
