---
title: "JMX Support"
slug: /jmx-support
---


This page describes the JMX management requirements.

Required MBeans
---------------

### Broker MBeans

We need MBeans for the core Broker types in the activemq.broker package to allow folks to see a broker's configuration, its connectors, currently connected clients & disconnect clients etc.

e.g. MBeans for

MBean|Description
---|---
Broker/BrokerContainer|The Message Broker itself
BrokerConnector|The inbound connector listening to new client connections
BrokerClient|A specific client connection

We have an old BrokerAdmin interface we should replace with proper MBeans ASAP.

In addition, we need to get access to the MessageContainer instances inside the Broker. These are a container per destination and QoS (e.g. 1 for Durable queue Foo and one for non-durable queue Foo).

So we'll need some way of viewing and looking up of thesse MC MBeans as they are created dynamically as users start using different destionations.

### Statistics

We implement J2EE Management Stats which add stats to each JMS resource; Connection, Session, Consumer, Producer (and we've added support to Destination too). So we need to expose those somehow in JMX so users can watch/view stats for JMS clients

Use cases
---------

Here's a list of end user use cases we need to support easily...

*   browsing the throughputs of connections/sessions/producers/consumers at the client side
*   browsing the throughputs of brokers on a per broker/destination/client basis
*   looking at the status of queues/topics (outstanding messages) to check for things filling up
*   emptying queues
*   replaying messsages from the dead letter queues
*   deleting messages on a queue/topic using a message ID
*   disconnecting clients on a broker; stopping a broker connection (to stop new inbound connections etc)
*   stopping brokers

The following are more nice to haves, but would be nice

*   viewing contents of queues/topics
*   starting/stopping remote brokers

Issues
------

*   do we need/want an MBean per JMS Connection/Session/Producer/Consumer? Am not sure why other than a way to grab stats; if so maybe some kinda stats MBean instead?

