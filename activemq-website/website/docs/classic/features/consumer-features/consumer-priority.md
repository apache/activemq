---
title: "Consumer Priority"
slug: /consumer-priority
---


### Background

As well as having a pluggable dispatch policy e.g. round robin, ActiveMQ also supports consumer priorities. This allows us to weight consumers to optimize network hops. For example, you typically want a broker to send messages to regular JMS consumers rather than to other brokers; there's no need to make unnecessary broker-to-broker hops if there are available consumers.

### Example

The priority for a consumer is set using [Destination Options](destination-options) as follows:
```
queue = new ActiveMQQueue("TEST.QUEUE?consumer.priority=10");
consumer = session.createConsumer(queue);
```
The range of priority values is: **`0`** to **`127`**. The highest priority is **`127`**. The default priority is **`0`**.

The broker orders a queue's consumers according to their priorities, dispatching messages to the highest priority consumers first. Once a particular consumer's prefetch buffer is full the broker will start dispatching messages to the consumer with the next lowest priority whose prefetch buffer is not full.

