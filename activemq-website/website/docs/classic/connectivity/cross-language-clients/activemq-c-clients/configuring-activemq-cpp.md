---
title: "Configuring ActiveMQ CPP"
slug: /configuring-activemq-cpp
---


The configuration of ActiveMQ is so slick, we decided to take a similar approach with ActiveMQ-CPP. All configuration is achieved via URI-encoded parameters, either on the connection or destinations. Through the URIs, you can configure virtually every facet of your ActiveMQ-CPP client. The tables below show the comprehensive set of parameters.

Connection URI Parameters
-----------------------------

#### Example Configuration

cf = new ActiveMQConnectionFactory(
    "tcp://localhost:61616?wireFormat=openwire&wireFormat.tightEncodingEnabled=true");

### **Socket Options**

Option Name|Default|Description
---|---|---
inputBufferSize|10000|The number of bytes in the buffered input stream's buffer
outputBufferSize|10000|The number of bytes in the buffered output stream's buffer
soLinger|0|Socket SOLinger value
soKeepAlive|false|Socket SOKeepAlive value
soReceiveBufferSize|-1|Socket receive buffer. If -1, use OS default.
soSendBufferSize|-1|Socket send buffer. If -1, use OS default.

### **Transport Options**

Option Name|Default|Description
---|---|---
commandTracingEnabled|false|If true, enables tracing of incoming and outgoing transport commands
tcpTracingEnabled|false|If true, enables tracing of raw network IO (in hex)
useAsyncSend|false|If true, enables asynchronous sending of messages.

### **Transaction Options**

Option Name|Default|Description
---|---|---
transaction.maxRedeliveryCount|5|Maximum number of redelivery attempts.

### **Wire Format Protocol Options**

Option Name|Default|Description
---|---|---
wireFormat|openwire|Selects the wire format to use. Out of the box, can be either stomp or openwire.
wireFormat.stackTraceEnabled|false|Should the stack trace of exception that occur on the broker be sent to the client? Only used by openwire protocol.
wireFormat.cacheEnabled|false|Should commonly repeated values be cached so that less marshalling occurs? Only used by openwire protocol.
wireFormat.tcpNoDelayEnabled|false|Does not affect the wire format, but provides a hint to the peer that TCP nodelay should be enabled on the communications Socket. Only used by openwire protocol.
wireFormat.sizePrefixDisabled|false|Should serialized messages include a payload length prefix? Only used by openwire protocol.
wireFormat.tightEncodingEnabled|false|Should wire size be optimized over CPU usage? Only used by the openwire protocol.

Destination URI Parameters
------------------------------

#### Example Configuration
```
d = session->createTopic("com.foo?consumer.prefetchSize=2000&consumer.noLocal=true");
```

### **General Options**
Option Name|Default|Description
---|---|---
consumer.prefetchSize|1000|The number of message the consumer will [prefetch](what-is-the-prefetch-limit-for).
consumer.maximumPendingMessageLimit|0|Use to control if messages are dropped if a [slow consumer](slow-consumer-handling) situation exists.
consumer.noLocal|false|Same as the noLocal flag on a Topic consumer. Exposed here so that it can be used with a queue.
consumer.dispatchAsync|false|Should the broker [dispatch messages asynchronously](consumer-dispatch-async) to the consumer.
consumer.retroactive|false|Is this a [Retroactive Consumer](retroactive-consumer).
consumer.selector|null|JMS Selector used with the consumer.
consumer.exclusive|false|Is this an [Exclusive Consumer](exclusive-consumer).
consumer.priority|0|Allows you to configure a [Consumer Priority](consumer-priority).

### **OpenWire-only Options**

Option Name|Default|Description
---|---|---
consumer.browser|false|
consumer.networkSubscription|false
consumer.optimizedAcknowledge|false|Enables an optimised acknowledgement mode where messages are acknowledged in batches rather than individually. Alternatively, you could use Session.DUPS\_OK\_ACKNOWLEDGE acknowledgement mode for the consumers which can often be faster. **WARNING** enabling this issue could cause some issues with auto-acknowledgement on reconnection
consumer.noRangeAcks|false
consumer.retroactive|false|Sets whether or not retroactive consumers are enabled. Retroactive consumers allow non-durable topic subscribers to receive old messages that were published before the non-durable subscriber started.
producer.dispatchAsyc|false

