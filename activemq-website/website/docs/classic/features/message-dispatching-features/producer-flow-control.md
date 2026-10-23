---
title: "Producer Flow Control"
slug: /producer-flow-control
---


Producer Flow Control
---------------------

In ActiveMQ 4.x flow control was implemented using TCP flow control. The underlying network connection of throttled consumers was suspended to enforce flow control limits. This strategy is very efficient but can lead to deadlocks if there are multiple producers and consumers sharing the same connection.

As of ActiveMQ 5.0, we can now individually flow control each producer on a shared connection without having to suspend the entire connection. By 'flow control' we mean that if the broker detects that the memory limit for the destination, or the temp- or file-store limits for the broker, have been exceeded, then the flow of messages can be slowed down. The producer will be either blocked until resources are available _or_ will receive a JMSException: this behaviour is configurable and described in the section below on `<systemUsage>`.

It's worth noting that the default `<systemUsage>` settings will cause the producer to _block_ when the `memoryLimit` or `<systemUsage>` limits are reached: this blocking behaviour is sometimes misinterpreted as a 'hung producer', when in fact the producer is simply diligently waiting until space is available.

*   Messages that are sent synchronously will automatically use per producer flow control; this applies generally to persistent messages which are sent synchronously _unless_ you enable the `useAsyncSend` flag.

*   Producers that use [Async Sends](async-sends) \- generally speaking, producers of non-persistent messages - don't bother waiting for any acknowledgement from the broker; so, if a memory limit has been exceeded, you will _not_ get notfied. If you do want to be aware of broker limits being exceeded, you will need to configure the ProducerWindowSize connection option so that even async messages are flow controlled per producer.
```
ActiveMQConnectionFactory connctionFactory = connctionFactory.setProducerWindowSize(1024000);
```
The ProducerWindowSize is the maximum number of bytes of data that a producer will transmit to a broker before waiting for acknowledgment messages from the broker that it has accepted the previously sent messages.

Alternatively, if you're sending non-persisted messages (which are by default sent async), and want to be informed if the queue or topic's memory limit has been breached, then you can simply configure the connection factory to 'alwaysSyncSend'. While this is going to be slower, it will ensure that your message producer is informed immediately of memory issues.

If you like, you can disable flow control for specific JMS queues and topics on the broker by setting the `producerFlowControl` flag to false on the appropriate destination policy in the Broker configuration - e.g.
```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry topic="FOO.>" producerFlowControl="false"/>
    </policyEntries>
  </policyMap>
</destinationPolicy>
```
see [Broker Configuration](xml-configuration).

Note that, since the introduction of the new file cursor in ActiveMQ 5.x, non-persisted messages are shunted into the temporary file store to reduce the amount of memory used for non-persistent messaging. As a result, you may find that a queue's memoryLimit is never reached, as the cursor doesn't use very much memory. If you really do want to keep all your non-persistent messages in memory, and stop producers when the limit is reached, you should configure the `<vmQueueCursor>`.
```
<policyEntry queue=">" producerFlowControl="true" memoryLimit="1mb">    
  <pendingQueuePolicy>
    <vmQueueCursor/>
  </pendingQueuePolicy>
</policyEntry>
```
The fragment above will ensure that all non-persistent queue messages are kept in memory, with each queue having a limit of 1Mb.

### How Producer Flow Control works

If you are sending a persistent message (so that a response of the [OpenWire](openwire) Message is expected then the broker will send the producer a [ProducerAck](http://activemq.apache.org/maven/5.9.0/apidocs/index.html) message. This informs the producer that the previous sending window has been processed, so that it can now send another window. Its kinda like consumer acks but in reverse.

#### Advantage

So a nice producer might wait for a producer ack before sending more data, to avoid flooding the broker (and forcing the broker to block the entire connection if a slow consumer occurs). To see how this works in source code, check out the [ActiveMQMessageProducer](http://activemq.apache.org/maven/5.9.0/apidocs/index.html) code.

Though a client can ignore the producer ACKs altogether and the broker should just stall the transport if it has to for slow consumer handling; though this does mean it'll stall the entire connection.

### Configure Client-Side Exceptions

An alternative to the indefinite blocking of the `send()` operation when no space is free on the broker is to instead configure that an exception to be thrown on the client-side. By configuring the `sendFailIfNoSpace` property to `true`, the broker will cause the `send()` operation to fail with a `javax.jms.ResourceAllocationException`, which will propagate to the client. Below is an example of this configuration:
```
<systemUsage>
 <systemUsage sendFailIfNoSpace="true">
   <memoryUsage>
     <memoryUsage limit="20 mb"/>
   </memoryUsage>
 </systemUsage>
</systemUsage>
```
The advantage of this property is that the client can catch the `javax.jms.ResourceAllocationException`, wait a bit and retry the `send()` operation instead of just hanging indefinitely.

Starting in version 5.3.1 the `sendFailIfNoSpaceAfterTimeout` property has been added. This property causes the `send()` operation to fail with an exception on the client-side, but only after waiting the given amount of time. If space on the broker is still not freed after the configured amount of time, only then does the `send()` operation fail with an exception to the client-side. Below is an example:
```
<systemUsage>
 <systemUsage sendFailIfNoSpaceAfterTimeout="3000">
   <memoryUsage>
     <memoryUsage limit="20 mb"/>
   </memoryUsage>
 </systemUsage>
</systemUsage>
```
The timeout is defined in milliseconds so the example above waits for three seconds before failing the `send()` operation with an exception to the client-side. The advantage of this property is that it will block for the configured amount of time instead of failing immediately or blocking indefinitely. This property offers not only an improvement on the broker-side, but also an improvement for the client so it can catch the exception, wait a bit and retry the `send()` operation.

Starting in version 5.16.0 the `sendFailIfNoSpace` and `sendFailIfNoSpaceAfterTimeout` can be configured on a per destination basis via [destination policies](per-destination-policies).

Disabling Flow Control
----------------------

A common requirement is to disable flow control so that message dispatching continues until all available disk is used up by pending messages (whether persistent or non persistent messaging is configured). To do this enable [Message Cursors](message-cursors).

System usage
------------

You can also slow down producers via some attributes on the `<systemUsage>` element. Take a look at the following example:
```
<systemUsage>
  <systemUsage>
    <memoryUsage>
      <memoryUsage limit="64 mb" />
    </memoryUsage>
    <storeUsage>
      <storeUsage limit="100 gb" />
    </storeUsage>
    <tempUsage>
      <tempUsage limit="10 gb" />
    </tempUsage>
  </systemUsage>
</systemUsage>
```
You can set limits of memory for `NON_PERSISTENT` messages, disk storage for `PERSITENT` messages and total usage for temporary messages, the broker will use before it slowdown producers. _Using the default settings shown above, the broker will block the `send()` call until some messages are consumed and space becomes available on the broker._ The default values are shown above, you will probably need to increase these values for your environment.

**PercentUsage**

Both StoreUsage and TempUsage support a percentLimit attribute where the limit is determined as a percentage of the total available. From version 5.15.x there is an additional related total attribute that can be used to explicitly set the total available such that the file system is not queried. This is useful in the case that only part of a disk partition is available to the broker or where the underlying file store reports > Long.MAX_VALUE available capacity (e.g: EFS) which will overflow the long return value of java.io.File#getTotalSpace. Note that when a total is specified, that actual data available will not be validated agains the file system, just the store usage relative to that absolute total.

