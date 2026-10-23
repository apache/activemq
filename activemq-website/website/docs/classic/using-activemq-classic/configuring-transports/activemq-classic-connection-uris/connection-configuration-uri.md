---
title: "Connection Configuration URI"
description: "Set ActiveMQ Classic JMS connection options on the broker URL with the jms. prefix, e.g. useAsyncSend, optimizeAcknowledge, prefetchPolicy and redeliveryPolicy."
slug: /connection-configuration-uri
---

Connection Configuration URI
----------------------------

An Apache ActiveMQ connection can be configured by explicitly setting properties on the [ActiveMQConnection](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQConnection.html) or [ActiveMQConnectionFactory](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html) objects themselves via the bean properties or using the following URI syntax.

### Example

You set the property called `jms.$PROPERTY` on a regular connection URI. For example you can set the `brokerURL` on your [ActiveMQConnectionFactory](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html) to the following value to enable [Async Sends](async-sends)
```
tcp://localhost:61616?jms.useAsyncSend=true
```

### Connection Options

> **Use the Correct Prefix!**
> 
> The following options should be prefixed with `jms.` when used on a `brokerURL` or a broker's transport connector.

Option Name|Default Value|Description
---|---|---
`alwaysSessionAsync`|`true`|When `true` a separate thread is used for dispatching messages for each Session in the Connection. A separate thread is always used when there's more than one session, or the session isn't in `Session.AUTO_ACKNOWLEDGE` or `Session.DUPS_OK_ACKNOWLEDGE` mode.
`alwaysSyncSend`|`false`|When `true` a `MessageProducer` will always use Sync sends when sending a Message even if it is not required for the Delivery Mode.
`auditDepth`|`2048`|The size of the message window that will be audited for duplicates and out of order messages.
`auditMaximumProducerNumber`|`64`|Maximum number of producers that will be audited.
`checkForDuplicates`|`true`|When `true` the consumer will check for duplicate messages and properly handle the message to make sure that it is not processed twice inadvertently.
`clientID`|`null`|Sets the JMS clientID to use for the connection.
`closeTimeout`|`15000`|Sets the timeout, in milliseconds, before a close is considered complete. Normally a `close()` on a connection waits for confirmation from the broker. This allows the close operation to timeout preventing the client from hanging when no broker is available.
`consumerExpiryCheckEnabled`|`true`|Controls whether message expiration checking is done in each `MessageConsumer` prior to dispatching a message. Disabling this check can lead to consumption of expired messages. (since 5.11).
`copyMessageOnSend`|`true`|Should a JMS message be copied to a new JMS Message object as part of the `send()` method in JMS. This is enabled by default to be compliant with the JMS specification. For a performance boost set to `false` if you do not mutate JMS messages after they are sent.
`disableTimeStampsByDefault`|`false`|Sets whether or not timestamps on messages should be disabled or not. For a small performance boost set to `false`.
`dispatchAsync`|`false`|Should the broker [dispatch messages asynchronously](consumer-dispatch-async) to the consumer?
`nestedMapAndListEnabled`|`true`|Controls whether [Structured Message Properties and MapMessages](structured-message-properties-and-mapmessages) are supported so that Message properties and `MapMessage` entries can contain nested Map and List objects. Available from version 4.1.
`objectMessageSerializationDefered`|`false`|When an object is set on an `ObjectMessage` the JMS spec requires the object be serialized by that set method. When `true` the object will not be serialized. The object may subsequently be serialized if the message needs to be sent over a socket or stored to disk.
`optimizeAcknowledge`|`false`|Enables an optimized acknowledgement mode where messages are acknowledged in batches rather than individually. Alternatively, you could use `Session.DUPS_OK_ACKNOWLEDGE` acknowledgement mode for the consumers which can often be faster. **WARNING**: enabling this issue could cause some issues with auto-acknowledgement on reconnection.
`optimizeAcknowledgeTimeOut`|`300`|If > 0, specifies the max time, in milliseconds, between batch acknowledgements when `optimizeAcknowledge` is enabled. (since 5.6).
`optimizedAckScheduledAckInterval`|`0`|If > 0, specifies a time interval upon which all the outstanding ACKs are delivered when optimized acknowledge is used so that a long running consumer that doesn't receive any more messages will eventually ACK the last few un-ACK'ed messages (since 5.7).
`optimizedMessageDispatch`|`true`|If `true` a larger prefetch limit is used - only applicable for durable topic subscribers.
`useAsyncSend`|`false`|Forces the use of [Async Sends](async-sends) which adds a massive performance boost; but means that the `send()` method will return immediately whether the message has been sent or not which could lead to message loss.
`useCompression`|`false`|Enables the use of compression on the message's body.
`useRetroactiveConsumer`|`false`|Sets whether or not retroactive consumers are enabled. Retroactive consumers allow non-durable topic subscribers to receive old messages that were published before the non-durable subscriber started.
`warnAboutUnstartedConnectionTimeout`|`500`|The timeout, in milliseconds, from the time of connection creation to when a warning is generated if the connection is not properly started via [Connection.start()](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/Connection.html#start()) and a message is received by a consumer. It is a very common gotcha to forget to [start the connection and then wonder why no messages are delivered](i-am-not-receiving-any-messages-what-is-wrong) so this option makes the default case to create a warning if the user forgets. To disable the warning just set the value to `< 0`.
`nonBlockingRedelivery`|`false`|Whether or not message redelivery (and any related delays) will block the delivery of other messages. Setting to `true` will allow messages to be delivered out of order.
`maxInflatedDataSizeRatio`| `10.0`        |Used to compute maxInflatedDataSize if [maxFrameSize](configuring-wire-formats) is configured on the wireformat. This ratio is used to determine the maximum allowed size of an uncompressed message body. maxInflatedDataSize is defined as `maxFrameSize * maxInflatedDataSizeRatio`.  For example, if maxFrameSize is 10MB then by default maxInflatedDataSize would be 100MB. (since v5.19.8 and v6.2.7)

### Nested Options

You can also configure nested objects on the connection object using the given prefixes. See the javadoc for a breakdown of each individual property.

Option Name| Object configured                                                                                                                                   |See Also
---|-----------------------------------------------------------------------------------------------------------------------------------------------------|---
`jms.blobTransferPolicy.*`| [BlobTransferPolicy](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/blob/BlobTransferPolicy.html)    |[Blob Message](#)
`jms.prefetchPolicy.*`| [ActiveMQPrefetchPolicy](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQPrefetchPolicy.html) |[What is the Prefetch Limit For?](what-is-the-prefetch-limit-for)
`jms.redeliveryPolicy.*`| [RedeliveryPolicy](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/RedeliveryPolicy.html)             |[Redelivery Policy](redelivery-policy)

For example you could set
```
tcp://localhost:61616?jms.prefetchPolicy.all=100&jms.redeliveryPolicy.maximumRedeliveries=5
```
