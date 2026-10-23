---
title: "Composite Destinations"
slug: /composite-destinations
---

As of version 1.1, ActiveMQ supports a technology we call _composite destinations_. This allows a single virtual JMS Destination to be used to represent a collection of JMS Destinations.

For example you can use composite destinations to send a message to 12 physical queues in one operation. Or send a message to one topic and one queue in one operation.

We do this using a simple separator of "," allowing a number of destinations to be specified when creating a destintation, or registering destinations in JNDI. e.g. the destination
```
FOO.A,FOO.B,FOO.C
```
Represents 3 different destinations. This could be used with a queue or topic to represent a set of 3 destinations. e.g.
```
// send to 3 queues as one logical operation
Queue queue = new ActiveMQQueue("FOO.A,FOO.B,FOO.C");
producer.send(queue, someMessage);
```
If you wish to mix and match the types of destination, you can use a prefix of `queue://` or `topic://` to differentiate the type of destination. e.g. to publish on a queue but also make a notification on a topic as well you could use
```
// send to queues and topic one logical operation
Queue queue = new ActiveMQQueue("FOO.A,topic://NOTIFY.FOO.A");
producer.send(queue, someMessage);
```
Composite destinations can also be configured on the [broker side](virtual-destinations), such that messages sent to a single destination by the client will be transparently copied to multiple physical destinations.
