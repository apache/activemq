---
title: "Configuring"
sidebar_position: 3
---
Configuring ActiveMQ-CPP
------------------------

The configuration of ActiveMQ is so slick, we decided to take a similar approach with ActiveMQ-CPP. All configuration is achieved via URI-encoded parameters, either on the connection or destinations. Through the URIs, you can configure virtually every facet of your ActiveMQ-CPP client. The tables below show the comprehensive set of parameters.

### **Connection URI Parameters**

#####  Example Configuration

This first example uses a standard TCP based transport.
```
cf = new ActiveMQConnectionFactory(
    "tcp://localhost:61616?wireFormat=openwire&wireFormat.tightEncodingEnabled=true");
```
For a more reliable connection use the Failover Transport and configure in alternate locations to connect to.
```
cf = new ActiveMQConnectionFactory(
    "failover://(tcp://localhost:61616,tcp://anotherhost:61616)?connection.useAsyncSend=true");
```
### Protocol Options

|Option Name|Description|
|---|---|
|tcp|Uses TCP/IP Sockets to connect to the broker.|
|ssl|Uses OpenSSL to secure TCP/IP sockets. **(Since v3.2.0)**|
|failover|Composite of other Transports, when a Transport connection fails, the Failover Transport connects to the next Transport in the list or retries the connection to the failed broker URI.|

#### **Socket Options**

|Option Name|Default|Description|
|---|---|---|
|inputBufferSize|10000|The number of bytes in the buffered input stream's buffer|
|outputBufferSize|10000|The number of bytes in the buffered output stream's buffer|
|soLinger|0|Socket SOLinger value|
|soKeepAlive|false|Socket SOKeepAlive value|
|soReceiveBufferSize|-1|Socket receive buffer. If -1, use OS default.|
|soSendBufferSize|-1|Socket send buffer. If -1, use OS default.|
|soConnectTimeout|-1|Socket connection timeout in microseconds. If -1, use OS default. **(Since ActiveMQ-CPP 2.2)**|
|tcpNoDelay|true|Sets the Socket level TCP_NODELAY option|

#### **General Transport Options**

|Option Name|Default|Description|
|---|---|---|
|transport.commandTracingEnabled|false|If true, enables tracing of incoming and outgoing transport commands|
|transport.tcpTracingEnabled|false|If true, enables tracing of raw network IO (in hex)|
|transport.useAsyncSend|false|If true, enables asynchronous sending of messages. ***WARNING: Using this could result in a loss of data if messages are pending and the client is shut down or crashes.**\* **(Deprecated Since ActiveMQ-CPP 2.2.2 has no effect as of this release, use connection.useAsyncSend instead)**|
|transport.ResponseCorrelator.maxResponseWaitTime|3000|Milliseconds to wait for responses from the broker. **(Removed as of ActiveMQ-CPP 2.2.1, use connection.sendTimeout instead)**|

#### **Failover Transport Options**

|Option Name|Default|Description|
|---|---|---|
|initialReconnectDelay|10|How long to wait if the initial attempt to connect to the broker fails.|
|maxReconnectDelay|30000|Maximum time that the transport waits before trying to connect to the Broker again.|
|useExponentialBackOff|true|Should the delay between connection attempt grow on each try up to the max reconnect delay|
|maxReconnectAttempts|0|Max number of times to attempt to reconnect before failing the transport, default is forever (0).|
|startupMaxReconnectAttempts|0|Max number of times to attempt to reconnect before failing the transport, default is forever (0). Only applies during the initial attempt to connect, after one successful connection the _maxReconnectAttempts_ flag applies from then on. **(Since v3.2.0)**|
|randomize|true|Should the transport attempt to connect to the listed Broker URI's in Random order.|
|backup|false|Should the Failover transport maintain hot backups.|
|backupPoolSize|1|If enabled, how many hot backup connections are made.|
|timeout|-1|If a send is blocked waiting on a failed connection to reconnect how long should it wait before failing the send, default is forever (-1).|

#### **Transaction Options**

|Option Name|Default|Description|
|---|---|---|
|transaction.maxRedeliveryCount|5|Maximum number of redelivery attempts. **(Removed in v3.2.0, see instead Connection Redelivery Policy)**|

#### **Connection Options**

|Option Name|Default|Description|
|---|---|---|
|connection.sendTimeout|0|Time to wait on Message Sends for a Response, default value of zero indicates to wait forever. Waiting forever allows the broker to have flow control over messages coming from this client if it is a fast producer or there is no consumer such that the broker would run out of memory if it did not slow down the producer. **Does not affect Stomp clients as the sends are ack'd by the broker.** **(Since ActiveMQ-CPP 2.2.1)**|
|connection.producerWindowSize|0|The ProducerWindowSize is the maximum number of bytes in memory that a producer will transmit to a broker before waiting for acknowledgement messages from the broker that it has accepted the previously sent messages. In other words, this how you configure the producer flow control window that is used for async sends where the client is responsible for managing memory usage. The default value of 0 means no flow control at the client. See also [Producer Flow Control](http://activemq.apache.orgFeatures/Message Dispatching FeaturesFeatures/Message Dispatching Features/Features/Message Dispatching Features/producer-flow-control.md). **This option only affects Openwire clients as Stomp does not have a means of notifying the clients of accepted messages asynchronously**. **(Since ActiveMQ-CPP 2.2.2)**|
|connnection.closeTimeout|15000|The amount of time to wait for a response from the broker when shutting down. Normally we want a response to indicate that the client has been disconnected cleanly, but we don't want to wait forever, however if you do, set this to zero. **(Since ActiveMQ-CPP 2.2.1)**|
|connection.useAsyncSend|false|Forces the use of Async Sends which adds a massive performance boost; but means that the send() method will return immediately whether the message has been sent or not which could lead to message loss. Normally only Persistant messages that are not part of a transaction are sent synchronously, using this options forces those messages to also be async. **(****Since ActiveMQ-CPP 2.2.2****)**|
|connection.alwaysSyncSend|false|Forces all messages that are sent to be sent synchronously overriding any usage of the useAsyncSend flag. This can reduce performance in some cases since the only messages we normally send synchronously are Persistent messages not sent in a transaction. This options guarantees that no send will return until the broker has acknowledge receipt of the message. **(****Since ActiveMQ-CPP 2.2.2****)**|
|connection.useCompression|false|Should outgoing messages have their body portion compressed using the ZLib compression algorithms **(Since v3.2.0)**|
|connection.dispatchAsync|true|Should the broker [dispatch messages asynchronously](http://activemq.apache.orgFeatures/Consumer FeaturesFeatures/Consumer Features/Features/Consumer Features/consumer-dispatch-async.md) to the consumer, this causes all consumers from this Connection to use this setting. **(Since v3.2.0)**|

##### **Connection Policy Options**

As of version 3.2.0 the Connection object supports policies that control various aspects its behaviour and of the Consumer objects it creates.

###### Prefetch Policy

|Option Name|Default|Description|
|---|---|---|
|cms.PrefetchPolicy.durableTopicPrefetch|100|Max prefetch for Durable Topics|
|cms.PrefetchPolicy.queueBrowserPrefetch|500|Max prefetch for a Queue Browser|
|cms.PrefetchPolicy.queuePrefetch|1000|Max prefetch for a Queue or Temp Queue|
|cms.PrefetchPolicy.topicPrefetch|65535|Max prefetch for a Topic or Temp Topic|

###### Redelivery Policy

|Option Name|Default|Description|
|---|---|---|
|cms.RedeliveryPolicy.backOffMultiplier|5.0|Used to extend the redelivery window when the _useExponentialBackOff option is enabled.|
|cms.RedeliveryPolicy.collisionAvoidancePercent|15|Percentage of randomness to add to redelivery backoff to keep multiple consumers in a rollback from redelivering at the same time.|
|cms.RedeliveryPolicy.initialRedeliveryDelay|1000|Amount of time to wait before starting redeliveries, in milliseconds.|
|cms.RedeliveryPolicy.maximumRedeliveries|6|Maximum number of time to attempt a redelivery before giving up and NAck'ing the message.|
|cms.RedeliveryPolicy.useCollisionAvoidance|false|Should the Random avoidance percentage be added into the redelivery delay.|
|cms.RedeliveryPolicy.useExponentialBackOff|false|Should redeliveries be slowed on each cycle.|

#### **Wire Format Protocol Options**

|Option Name|Default|Description|
|---|---|---|
|wireFormat|openwire|Selects the wire format to use. Out of the box, can be either stomp or openwire.|
|wireFormat.stackTraceEnabled|false|Should the stack trace of exception that occur on the broker be sent to the client? Only used by openwire protocol.|
|wireFormat.cacheEnabled|false|Should commonly repeated values be cached so that less marshalling occurs? Only used by openwire protocol.|
|wireFormat.tcpNoDelayEnabled|true|Does not affect the wire format, but provides a hint to the peer that TCP nodelay should be enabled on the communications Socket. Only used by openwire protocol.|
|wireFormat.sizePrefixDisabled|false|Should serialized messages include a payload length prefix? Only used by openwire protocol.|
|wireFormat.tightEncodingEnabled|false|Should wire size be optimized over CPU usage? Only used by the openwire protocol.|
|wireFormat.maxInactivityDuration|30000|The maximum inactivity duration (before which the socket is considered dead) in milliseconds. On some platforms it can take a long time for a socket to appear to die, so we allow the broker to kill connections if they are inactive for a period of time. Use by some transports to enable a keep alive heart beat feature. Set to a value <= 0 to disable inactivity monitoring.|
|maxInactivityDurationInitalDelay|10000|The initial delay in starting the maximum inactivity checks (and, yes, the word 'Inital' is supposed to be misspelled like that)|

### **Destination URI Parameters**

##### Example Configuration
```
d = session->createTopic("com.foo?consumer.prefetchSize=2000&consumer.noLocal=true");
```
#### **General Options**

|Option Name|Default|Description|
|---|---|---|
|consumer.prefetchSize|1000|The number of message the consumer will [prefetch](http://activemq.apache.orgCommunity/FAQ/Using Apache ActiveMQCommunity/FAQ/Using Apache ActiveMQ/Community/FAQ/Using Apache ActiveMQ/what-is-the-prefetch-limit-for.md).|
|consumer.maximumPendingMessageLimit|0|Use to control if messages are dropped if a [slow consumer](http://activemq.apache.orgFeatures/Consumer FeaturesFeatures/Consumer Features/Features/Consumer Features/slow-consumer-handling.md) situation exists.|
|consumer.noLocal|false|Same as the noLocal flag on a Topic consumer. Exposed here so that it can be used with a queue.|
|consumer.dispatchAsync|false|Should the broker [dispatch messages asynchronously](http://activemq.apache.orgFeatures/Consumer FeaturesFeatures/Consumer Features/Features/Consumer Features/consumer-dispatch-async.md) to the consumer.|
|consumer.retroactive|false|Is this a [Retroactive Consumer](http://activemq.apache.orgFeatures/Consumer FeaturesFeatures/Consumer Features/Features/Consumer Features/retroactive-consumer.md).|
|consumer.selector|null|JMS Selector used with the consumer.|
|consumer.exclusive|false|Is this an [Exclusive Consumer](http://activemq.apache.orgFeatures/Consumer Features/exclusive-consumer.md).|
|consumer.priority|0|Allows you to configure a [Consumer Priority](http://activemq.apache.orgFeatures/Consumer FeaturesFeatures/Consumer Features/Features/Consumer Features/consumer-priority.md).|

#### **OpenWire-only Options**

|Option Name|Default|Description|
|---|---|---|
|consumer.browser|false||
|consumer.networkSubscription|false||
|consumer.optimizedAcknowledge|false|Enables an optimised acknowledgement mode where messages are acknowledged in batches rather than individually. Alternatively, you could use Session.DUPS_OK_ACKNOWLEDGE acknowledgement mode for the consumers which can often be faster. **WARNING** enabling this issue could cause some issues with auto-acknowledgement on reconnection|
|consumer.noRangeAcks|false||
|consumer.retroactive|false|Sets whether or not retroactive consumers are enabled. Retroactive consumers allow non-durable topic subscribers to receive old messages that were published before the non-durable subscriber started.|

