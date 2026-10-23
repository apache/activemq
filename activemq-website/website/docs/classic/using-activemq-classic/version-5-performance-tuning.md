---
title: "Performance Tuning"
description: "Improve ActiveMQ Classic throughput with useAsyncSend, larger consumer prefetch sizes, optimizeAcknowledge batching and alwaysSessionAsync=false."
slug: /version-5-performance-tuning
---


For a more complete overview see [Performance](performance).

There are trade-offs between performance and reliabilty.  
By default, activemq strikes a balance between the two, so there are some things you can change to increase throughput.

Async publishing
----------------

First some background:

When an ActiveMQ message producer sends a non-persistent message, its dispatched asynchronously (fire and forget) - but for persistent messages, the publisher will block until it gets a notification that the message has been processed (saved to the store - queued to be dispatched to any active consumers etc) by the broker. messages are dispatched with delivery mode set to be persistent by default (which is required by the JMS spec). So if you are sending messages on a Topic, the publisher will block by default (even if there are no durable subscribers on the topic) until the broker has returned a notification.

So if you looking for good performance with topic messages, either set the delivery mode on the publisher to be non-persistent, or set the **useAsyncSend** property on the ActiveMQ ConnectionFactory to be **true**.

Pre-fetch sizes for Consumers
-----------------------------

ActiveMQ will push as many messages to the consumer as fast as possible, where they will be queued for processing by an ActiveMQ Session. The maximum number of messages that ActiveMQ will push to a Consumer without the Consumer processing a message is set by the pre-fetch size. You can improve throughput by running ActiveMQ with larger pre-fetch sizes. Pre-fetch sizes are determined by the ActiveMQPrefetchPolicy bean, which is set on the ActiveMQ ConnectionFactory.

Default values:

consumer type|default value
---|---
queue|1000
queue browser|500
topic|32766
durable topic|1000

Optimized Acknowledge
---------------------

When consuming messages in auto acknowledge mode (set when creating the consumers' session), ActiveMQ will acknowledge receipt of messages back to the broker in batches (to improve performance). The batch size is 50% of the prefetch limit for the Consumer. You switch batch acknowledgment off by setting the **optimizeAcknowledge** property on the ActiveMQ ConnectionFactory to be **false**

Straight through Session Consumption
------------------------------------

By default, a Consumer's session will dispatch messages to the consumer in a separate thread. If you are using Consumers with auto acknowledge, you can increase throughput by passing messages straight through the Session to the Consumer by setting the **alwaysSessionAsync** property on the ActiveMQ ConnectionFactory to be **false**

File based Persistence
----------------------

The default persistence store is the [AMQ Message Store](amq-message-store)

