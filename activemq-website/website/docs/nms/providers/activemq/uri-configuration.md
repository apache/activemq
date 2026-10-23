---
title: "NMS.ActiveMQ URI Configuration"
sidebar_position: 3
---

Configuring NMS.ActiveMQ
------------------------

All configuration is achieved via URI-encoded parameters, either on the connection or destinations. Through the URIs, you can configure virtually every facet of your NMS.ActiveMQ client. The tables below show the comprehensive set of parameters.

## Connection URI Parameters
---
#### Example Configuration

Using the Generic NMSConnectionFactory class would look as follows:
```
var cf = new NMSConnectionFactory("activemq:tcp://localhost:61616?wireFormat.tightEncodingEnabled=true");
```
You can also use the ActiveMQ ConecctionFactory implementation directory:
```
var cf = new Apache.NMS.ActiveMQ.ConnectionFactory("tcp://localhost:61616?wireFormat.tightEncodingEnabled=true");
```
## Protocol Options
---

|Option|Description|
|-----|-----|
|tcp|Uses TCP/IP Sockets to connect to the Broker.|
|ssl|Uses TCP/IP Sockets to connect to the Broker with an added SSL layer.|
|discovery|Uses The Discovery Transport to find a Broker|
|failover|Uses the Failover Transport to connect and reconnect to one or more Brokers|

## Transport Options
---
### TCP Transport Options

|Option|Default|Description|
|-----|-----|-----|
|transport.useLogging|false|Log data that is sent across the Transport.|
|transport.receiveBufferSize|8192|Amount of Data to buffer from the Socket|
|transport.sendBufferSize|8192|Amount of Data to buffer before writing to the Socket|
|transport.receiveTimeout|0|Time to wait for more data, zero means wait infinitely|
|transport.sendTimeout|0|Timeout on sends, 0 means wait forever for completion|
|transport.requestTimeout|0|Time to wait before a Request Command is considered to have failed|

### Discovery Transport Options

### Failover Transport Options

Prior to NMS.ActiveMQ v1.4.0 the failover transport options did not use the `transport.*` prefix.

|Option|Default|Description|
|-----|-----|-----|
|transport.timeout|-1|Time that a send operation blocks before failing.|
|transport.initialReconnectDelay|10|Time in Milliseconds that the transport waits before attempting to reconnect the first time.|
|transport.maxReconnectDelay|30000|The max time in Milliseconds that the transport will wait before attempting to reconnect.|
|transport.backOffMultiplier|2|The amount by which the reconnect delay will be multiplied by if useExponentialBackOff is enabled.|
|transport.useExponentialBackOff|true|Should the delay between connection attempt grow on each try up to the max reconnect delay.|
|transport.randomize|true|Should the Uri to connect to be chosen at random from the list of available Uris.|
|transport.maxReconnectAttempts|0|Maximum number of time the transport will attempt to reconnect before failing (0 means infinite retries)|
|transport.startupMaxReconnectAttempts|0|Maximum number of time the transport will attempt to reconnect before failing when there has never been a connection made. (0 means infinite retries) **(included in NMS.ActiveMQ v1.5.0+)**|
|transport.reconnectDelay|10|The delay in milliseconds that the transport waits before attempting a reconnection.|
|transport.backup|false|Should the Failover transport maintain hot backups.|
|transport.backupPoolSize|1|If enabled, how many hot backup connections are made.|
|transport.trackMessages|false|keep a cache of in-flight messages that will flushed to a broker on reconnect|
|transport.maxCacheSize|256|Number of messages that are cached if trackMessages is enabled.|
|transport.updateURIsSupported|true|Update the list of known brokers based on BrokerInfo messages sent to the client.|

## Connection Options
---
Connection options can either be set via the `connection.*` prefix or the `nms.*` prefix similar to the java client's `jms.*` prefix settings.

|Option|Default|Description|
|-----|-----|-----|
|connection.AsyncSend|false|Are message sent Asynchronously.|
|connection.AsyncClose|true|Should the close command be sent Asynchronously|
|connection.AlwaysSyncSend|false|Causes all messages a Producer sends to be sent Asynchronously.|
|connection.CopyMessageOnSend|true|Copies the Message objects a Producer sends so that the client can reuse Message objects without affecting an in-flight message.|
|connection.ProducerWindowSize|0|The ProducerWindowSize is the maximum number of bytes in memory that a producer will transmit to a broker before waiting for acknowledgement messages from the broker that it has accepted the previously sent messages. In other words, this how you configure the producer flow control window that is used for async sends where the client is responsible for managing memory usage. The default value of 0 means no flow control at the client. See also [Producer Flow Control](/components/classic/documentation/producer-flow-control)|
|connection.useCompression|false|Should message bodies be compressed before being sent.
|connection.sendAcksAsync|false|Should message acks be sent asynchronously|
|connection.messagePrioritySupported|true|Should messages be delivered to the client based on the value of the Message Priority header.|
|connection.dispatchAsync|false|Should the broker [dispatch messages asynchronously](/components/nms/providers/activemq/advanced-features/consumer-dispatch-async) to the connection's consumers.|
|connection.watchTopicAdvisories|true|Should the client watch for advisory messages from the broker to track the creation and deletion of temporary destinations.|

### OpenWire Options

|Option|Default|Description|
|-----|-----|-----|
|wireFormat.stackTraceEnabled|false|Should the stack trace of exception that occur on the broker be sent to the client? Only used by openwire protocol.|
|wireFormat.cacheEnabled|false|Should commonly repeated values be cached so that less marshalling occurs? Only used by openwire protocol.|
|wireFormat.tcpNoDelayEnabled|false|Does not affect the wire format, but provides a hint to the peer that TCP nodelay should be enabled on the communications Socket. Only used by openwire protocol.|
|wireFormat.sizePrefixDisabled|false|Should serialized messages include a payload length prefix? Only used by openwire protocol.|
|wireFormat.tightEncodingEnabled|false|Should wire size be optimized over CPU usage? Only used by the openwire protocol.|
|wireFormat.maxInactivityDuration|30000|The maximum inactivity duration (before which the socket is considered dead) in milliseconds. On some platforms it can take a long time for a socket to appear to die, so we allow the broker to kill connections if they are inactive for a period of time. Use by some transports to enable a keep alive heart beat feature. Set to a value <= 0 to disable inactivity monitoring.|
|maxInactivityDurationInitalDelay|10000|The initial delay in starting the maximum inactivity checks (and, yes, the word 'Inital' is supposed to be misspelled like that)|

## Destination URI Parameters
---
#### Example Configuration

```
d = session.CreateTopic("com.foo?consumer.prefetchSize=2000&consumer.noLocal=true");
```

### General Options
---

|Option|Default|Description|
|-----|-----|-----|
|consumer.prefetchSize|1000|The number of message the consumer will [prefetch](/components/classic/documentation/what-is-the-prefetch-limit-for).  **Removed in v1.7.0 use connection prefetch policy instead.**|
|consumer.maximumPendingMessageLimit|0|Use to control if messages are dropped if a [slow consumer](/components/classic/documentation/slow-consumer-handling) situation exists.|
|consumer.noLocal|false|Same as the noLocal flag on a Topic consumer. Exposed here so that it can be used with a queue.|
|consumer.dispatchAsync|false|Should the broker [dispatch messages asynchronously](/components/nms/providers/activemq/advanced-features/consumer-dispatch-async) to the consumer.|
|consumer.retroactive|false|Is this a [Retroactive Consumer](/components/nms/providers/activemq/advanced-features/retroactive-consumers).|
|consumer.selector|null|JMS Selector used with the consumer.|
|consumer.exclusive|false|Is this an [Exclusive Consumer](/components/nms/providers/activemq/advanced-features/exclusive-consumers).|
|consumer.priority|0|Allows you to configure a [Consumer Priority](/components/nms/providers/activemq/advanced-features/consumer-priority).

### OpenWire-only Options
---

|Option|Default|Description|
|-----|-----|-----|
|consumer.browser|false||
|consumer.networkSubscription|false||
|consumer.optimizedAcknowledge|false|Enables an optimised acknowledgement mode where messages are acknowledged in batches rather than individually. Alternatively, you could use `Session.DUPS_OK_ACKNOWLEDGE` acknowledgement mode for the consumers which can often be faster. **WARNING** enabling this issue could cause some issues with auto-acknowledgement on reconnection|
|consumer.noRangeAcks|false||
|consumer.retroactive|false|Sets whether or not retroactive consumers are enabled. Retroactive consumers allow non-durable topic subscribers to receive old messages that were published before the non-durable subscriber started.|
