---
title: "ActiveMQ Message Properties"
slug: /activemq-classic-message-properties
---


**ActiveMQ Messages support the following default message properties:**

Message Attributes Accessed as Properties:
------------------------------------------

Property Name|Type|Default Value|Description
---|---|---|---
`JMSDestination`|`javax.jms.Destination`|_Set by the producer_|Destination used by the producer.
`JMSReplyTo`|`javax.jms.Destination`|`null`|User defined.
`JMSType`|`String`|`""`|User defined.
`JMSDeliveryMode`|`int`|`DeliveryMode.PERSISTENT`|Indicator if messages should be persisted.
`JMSPriority`|`int`|`4`|Value from `0-9`.
`JMSMessageID`|`String`|`unique`|Unique identifier for the message.
`JMSTimestamp`|`long`|_Time the message was sent_|Time in milliseconds.
`JMSCorrelationID`|`String`|`null`|User defined.
`JMSExpiration`|`long`|`0`|Time in milliseconds to expire the message. A value of `0` means never expire.
`JMSRedelivered`|`boolean`|`false`|`true` if the message is being resent to the consumer, persisted via [persistJMSRedelivered](per-destination-policies)

JMS Defined:
------------

Property Name|Type|Default Value|Description
---|---|---|---
`JMSXDeliveryCount`|`int`|`0`|Number of attempts to send the message.
`JMSXGroupID`|`String`|`null`|Identity of the message group.
`JMSXGroupSeq`|`int`|`0`|Sequence number of the message.
`JMSXProducerTXID`|`String`|`null`|Transaction identifier.

ActiveMQ Defined:
-------------------------

Property Name|Type|Default Value|Description
---|---|---|---
`JMSActiveMQBrokerInTime`|`long`|`0`|Time stamp (in milliseconds) for when the message arrived at the broker.
`JMSActiveMQBrokerOutTime`|`long`|`0`|Time stamp (in milliseconds) for when the message left the broker.

