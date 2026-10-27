---
title: "Stomp URI Configuration"
sidebar_position: 2
---

Configuring NMS.Stomp
---------------------

All configuration is achieved via URI-encoded parameters, either on the connection or destinations. Through the URIs, you can configure virtually every facet of your NMS.Stomp client. The tables below show the comprehensive set of parameters.

### **Connection URI Parameters**

#####  Example Configuration

Using the Generic NMSConnectionFactory class would look as follows:
```
var cf = new NMSConnectionFactory("stomp:tcp://localhost:61613");
```
You can also use the Stomp ConecctionFactory implementation directory:
```
var cf = new Apache.NMS.Stomp.ConnectionFactory("tcp://localhost:61613");
```
### Protocol Options

|Option Name|Description|
|---|---|
|tcp|Uses TCP/IP Sockets to connect to the Broker.|
|ssl|Uses TCP/IP Sockets to connect to the Broker with an added SSL layer (Not available on .NETCF).|
|failover|Uses the Failover Transport to connect and reconnect to one or more Brokers|

You can add the failover option using the Generic NMSConnectionFactory class like this:
```
var cf = new NMSConnectionFactory("stomp:failover:tcp://localhost:61613");
```
You can also use the Stomp ConecctionFactory implementation directory:
```
var cf = new Apache.NMS.Stomp.ConnectionFactory("failover:tcp://localhost:61613");
```
#### **Transport Options**

##### **TCP Transport Options**

|Option Name|Default|Description|
|---|---|---|
|transport.useLogging|false|Log data that is sent across the Transport.|
|transport.receiveBufferSize|8192|Amount of Data to buffer from the Socket|
|transport.sendBufferSize|8192|Amount of Data to buffer before writing to the Socket|
|transport.receiveTimeout|0|Time to wait for more data, zero means wait infinitely|
|transport.sendTimeout|0|Timeout on sends, 0 means wait forever for completion|
|transport.requestTimeout|0|Time to wait before a Request Command is considered to have failed|

##### Failover Transport Options

Prior to NMS.Stomp v1.4.0 the failover transport options did not use the `transport.*` prefix.

|Option Name|Description|
|---|---|
|transport.timeout|-1|Time that a send operation blocks before failing.|
|transport.initialReconnectDelay|10|Time in Milliseconds that the transport waits before attempting to reconnect the first time.|
|transport.maxReconnectDelay|30000|The max time in Milliseconds that the transport will wait before attempting to reconnect.|
|transport.backOffMultiplier|2|The amount by which the reconnect delay will be multiplied by if useExponentialBackOff is enabled.|
|transport.useExponentialBackOff|true|Should the delay between connection attempt grow on each try up to the max reconnect delay.|
|transport.randomize|true|Should the Uri to connect to be chosen at random from the list of available Uris.|
|transport.maxReconnectAttempts|0|Maximum number of time the transport will attempt to reconnect before failing (0 means infinite retries)|
|transport.startupMaxReconnectAttempts|0|Maximum number of time the transport will attempt to reconnect before failing when there has never been a connection made. (0 means infinite retries) **(included in NMS.Stomp v1.5.0+)**|
|transport.reconnectDelay|10|The delay in milliseconds that the transport waits before attempting a reconnection.|

#### **Connection Options**

|Option Name|Default|Description|
|---|---|---|
|connection.AsyncSend|false|Are message sent Asynchronously.|
|connection.AsyncClose|true|Should the close command be sent Asynchronously|
|connection.AlwaysSyncSend|false|Causes all messages a Producer sends to be sent Asynchronously.|
|connection.CopyMessageOnSend|true|Copies the Message objects a Producer sends so that the client can reuse Message objects without affecting an in-flight message.|
|connection.useCompression|false|Should message bodies be compressed before being sent.|
|connection.sendAcksAsync|false|Should message acks be sent asynchronously|
|connection.messagePrioritySupported|true|Should messages be delivered to the client based on the value of the Message Priority header.|
|connection.dispatchAsync|false|Should the broker [dispatch messages asynchronously](/components/classic/documentation/consumer-dispatch-async) to the connection's consumers.|

##### **Stomp Wire Protocol Options**

Be Careful

These values are currently not supported but are planned for a later release.

|Option Name|Default|Description|
|---|---|---|
|wireFormat.maxInactivityDuration|30000|The maximum inactivity duration (before which the socket is considered dead) in milliseconds. On some platforms it can take a long time for a socket to appear to die, so we allow the broker to kill connections if they are inactive for a period of time. Use by some transports to enable a keep alive heart beat feature. Set to a value <= 0 to disable inactivity monitoring.|
|wireFormat.maxInactivityDurationInitialDelay|10000|The initial delay in starting the maximum inactivity checks (and, yes, the word 'Initial' is supposed to be misspelled like that)|

### **Destination URI Parameters**

##### Example Configuration
```
d = session.CreateTopic("com.foo?consumer.prefetchSize=2000&consumer.noLocal=true");
```
#### **General Options**

|Option Name|Default|Description|
|---|---|---|
|consumer.prefetchSize|1000|The number of message the consumer will [prefetch](/components/classic/documentation/what-is-the-prefetch-limit-for).|
|consumer.noLocal|false|Same as the noLocal flag on a Topic consumer. Exposed here so that it can be used with a queue.|
|consumer.dispatchAsync|false|Should the broker [dispatch messages asynchronously](/components/classic/documentation/consumer-dispatch-async) to the consumer.|
|consumer.retroactive|false|Is this a [Retroactive Consumer](/components/classic/documentation/retroactive-consumer).|
|consumer.selector|null|JMS Selector used with the consumer.|
|consumer.exclusive|false|Is this an [Exclusive Consumer](/components/classic/documentation/exclusive-consumer).|
