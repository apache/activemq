---
title: "Consumer Dispatch Async"
slug: /consumer-dispatch-async
---

Overview
--------

From ActiveMQ v4: the choice of a broker performing synchronous or asynchronous dispatch to a consumer has become more configurable. It is now configured as a default value on the [connection URI, Connection and ConnectionFactory](connection-configuration-uri) as well as with being customizable on a per consumer basis via the [Destination Options](destination-options) instead previously just being a transport server setting.

This makes more sense since you want to do asynchronous message delivery to slower consumers, but synchronous message delivery for faster consumers (to avoid the synchronization and context switching costs of adding another SEDA queue). The downside to using synchronous message delivery is that the producer is more likely to block if there is a slow consumer that he is dispatching messages to.

The default setting is `dispatchAsync=true` which is the best setting for high performance. If you want to deal better with slow consumers you will want to enable this setting. If you want better throughput and the chances of having a slow consumer are low, you may want to change this to `dispatchAsync=false`.

### Configuring Async Dispatch at the ConnectionFactory Level
```
((ActiveMQConnectionFactory)connectionFactory).setDispatchAsync(false);
```

### Configuring Dispatch Async at the Connection Level

Configuring the `dispatchAsync` setting at this level overrides the settings at the connection factory level.

### Configuring Dispatch Async at the Consumer Level using the Destination URI

Configuring the `dispatchAsync` using [Destination Options](destination-options) overrides the settings at the connection and connection factory level.
```
queue = new ActiveMQQueue("TEST.QUEUE?consumer.dispatchAsync=false");
consumer = session.createConsumer(queue);
```

### Disabling Async Dispatch on Broker's Transport Connector

It is possible to disable async dispatch on a specific transport connector via the `disableAsyncDispatch` property. Once disabled for this transport, it cannot be enabled by individual clients.
```
<transportConnector name="openwire" uri="tcp://0.0.0.0:61616" disableAsyncDispatch="true"/>
```
