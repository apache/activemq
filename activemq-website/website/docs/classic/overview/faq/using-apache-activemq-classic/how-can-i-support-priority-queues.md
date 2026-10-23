---
title: "How can I support priority queues"
slug: /how-can-i-support-priority-queues
---

How can I support priority queues?
----------------------------------

### Use Message Priority

A common requirement is to support priority consumption; so high priority messages are consumed before low priority.

In version 5.4 priority queues are supported. Both the message cursors and the message stores (KahaDB and JDBC) support message priority. The support is disabled by default so it needs to be be enabled using [per destination policies](per-destination-policies) through xml configuration, in the example below, 'prioritizedMessages' is enabled for all queues.
```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue=">" prioritizedMessages="true"/>
      ...
```
The full range of priority values (0-9) are supported by the [JDBC](jdbc-support) message store. For [KahaDB](kahadb) three priority categories are supported, Low (< 4), Default (= 4) and High (> 4).

Since the message cursors (and client side) implement strict ordering of priorities, it's possible to observe strict priority ordering if message dispatching can happen from the cache and not have to hit the disk (i.e., your consumers are fast enough to keep up with producers), or if you're using non-persistent messages that never have to flush to disk (using the FilePendingMessageCursor). However, once you hit a situation where consumers are slow, or producers are just significantly faster, you'll observe that the cache will fill up (possibly with lower priority messages) while higher priority messages get stuck on disk and not available until they're paged in. In this case, you can make a decision to tradeoff optimized message dispatching for priority enforcement. You can disable the cache, message expiration check, and lower you consumer prefetch to 1 to ensure getting the high priority messages from the store ahead of lower priority messages Note, this sort of tradeoff can have significant performance implications, so you must test your scenarios thoroughly. :
```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue=">" prioritizedMessages="true" useCache="false" expireMessagesPeriod="0" queuePrefetch="1" />
      ...
```

### Alternative strategies

#### Use Selectors

You can have say 100 consumers using a selector to find the high priority stuff
```
JMSPriority > 6
```
then have 50 consumers doing average or above
```
JMSPriority >= 4
```
Then say 10 consumers consuming all messages (so all priorities). Then this way you'll have a pool of threads always processing high priority messages - giving you very efficient priority based dispatching of messages without ActiveMQ having to batch up messages and reorder them before dispatching them.

#### Use Resequencer

You can reorder messages on some input queue A and send them to queue B in sorted order to avoid having to change your clients. This avoids the need to use selectors in your application as shown above.

To do this use the [Resequencer](http://activemq.apache.org/camel/resequencer.html) from the [Enterprise Integration Patterns](enterprise-integration-patterns)

