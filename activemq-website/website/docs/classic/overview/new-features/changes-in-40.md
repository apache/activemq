---
title: "Changes in 4.0"
slug: /changes-in-40
---

### New Features in 4.0

*   [MasterSlave](masterslave) provides support for continuous availability and fault tolerance of brokers to be able to handle catastrophic hardware failure and not loose a message (or get duplicates).
*   A new [Exclusive Consumer](exclusive-consumer) feature allows you to pin message processing to a single consumer in a cluser of consumers.
*   A new [Message Groups](message-groups) feaure allows you load balance messages accross a set of consumers but also garantee the message order for messages within a message group.
*   A new [Total Ordering](total-ordering) feature to allow all consumers on a topic to see messages in the same order.
*   New [JMX Management](how-can-i-monitor-activemq-classic) and monitoring capabilities. You can now see statistics for each broker, destination, connector and connection!
*   Improved [Security](security) plugin which provides JAAS support for authentication along with a pluggable strategy for authorization together with a default XML based implementation.
*   A new [OpenWire C Client](openwire-c-client) is now available. This client talks the same wire protocol that the standard java client uses so every messaging broker feature available to the java client is available to the c client.
*   An experimental [OpenWire dotNet](/components/nms) is available, written in pure C# along with a JMS-like API for working on the .Net platform with ActiveMQ
*   Queues can now be loaded up with persistent messages without locking up the broker. Persistent messages are now swapped out of memory when no consumer needs it soon.
*   A new [Consumer Priority](consumer-priority) feature allows you to build location affinity by assignin a priority to consumers. The broker can then dispatch messages to higher priority consumers before dispatching to lower priority consumers.
*   A configurable per [Consumer Dispatch Async](consumer-dispatch-async) flag which allows you to configure how messages are sent by the broker to a consumer. This controls if the broker uses [SEDA](seda) or [STP](#) style dispaching.
*   A new plug-able topic [Subscription Recovery Policy](subscription-recovery-policy) which allows you to configure how many transient messages are replayed when a [Retroactive Consumer](retroactive-consumer) is created.
*   A new [Retroactive Consumer](retroactive-consumer) feature allows topic consumers to "go back in time" so that it receives old messages when the subscription is activated. If the consumer is a durable consumer, he recover all the messages that are still in the persistent store.
*   [Per Destination Policies](per-destination-policies) allow you configure the behavior of destinations.
*   The broker now supports per destination plugable [Dispatch Policies](dispatch-policies) so that you can choose the distribution algorithm used to send messages to a consumer.
*   The broker now supports two new types of connectors:
    *   [The JMS Connector](jms-to-jms-bridge) is used to establish connection to external JMS providers so that messages can be bridged between the system.
    *   [The Proxy Connector](the-proxy-connector). Is used to proxy connections to another broker.
*   [Slow Consumer Handling](slow-consumer-handling) allows you to discard old messages for slow consumers on non-durable topics to avoid slowing down fast consumers
*   You can now specify [Destination Options](destination-options) that allow you to do extend configuration of consumers.
*   Conumsers now use [Optimized Acknowledgement](optimized-acknowledgement) by default to which results in increased performance.

### API/Configuration chanages

*   as part of the move to Apache, the package name is now **org.apache.activemq** and not **org.activemq**.
*   the [Xml Configuration](xml-configuration) has changed a little; mostly its now in the ActiveMQ namespace and has a generated XSD and documentation.
*   the _reliable_ transport has been renamed to _failover_ to make it more clear what it does; we're working on a separate DR mechanism to provide data centre resilliance. So if you wish to connect to one of a number of URIs try
    ```
    failover:tcp://host1:port1,tcp://host2:port2
    ```
*   The configuration options of transports have changed. See [ActiveMQ Connection URIs](activemq-classic-connection-uris) for a detailed guide of of all the options.
*   The spring package has gone; we now use [XBean](https://geronimo.apache.org/xbean/) to configure ActiveMQ. See the org.activemq.xbean.BrokerFactoryBean if you want a factory bean to use in regular spring instead of the org.activemq.spring.BrokerFactoryBean. See [Configuring Brokers](configuring-brokers) for more information on the new XML syntax.
*   ActiveMQTopic and ActiveMQQueue are now in the org.activemq.command package.
*   If you were creating a broker in Java code, the BrokerContainer has been replaced with BrokerService which is easier to use now.
*   The connection URL options have changed slightly to provided more persise configuration options of the transport and wireformat and to allow validation of the options.
*   [Message Redelivery and DLQ Handling](message-redelivery-and-dlq-handling) has been re-implemnted. Currently all messages sent poison messages are sent to a single DQL.
*   The [JMS Streams](jms-streams) API has changed.

### General Changes

*   The JDBC persistence adapter now uses JDBC statement batching to increase it's performance with the database. This should reduce the amount of time a checkpoint takes.
*   QueueBrowsers now play nicely with a queue that is currently being consumed from. It gives you a true snapshot of what the queue looked like when you created the browser and it does not affect the dispatching of messages to active consumers.
*   we no longer have hand-crafted marshalling code any more; its all based on [OpenWire](openwire) and autogenerated from the open wire commands in the org.activemq.command package
*   The network bridges used for broker to broker messaging now use a lower level ActiveMQ command and transport API instead of the JMS API, this allows them to use more optimizations and have a lower per bridge resource consumption while letting the JMS client API implementation reduce it's complexity.
*   Two types of network bridges are now supported:
    *   A simple forwardng bridge - sends all messages as soon as possible to the remote broker. Great you know the usage patterns up front and allways want to do store and forward to a central broker.
    *   A demand based forwarding bridge (same type of bridge that was using in ActiveMQ 3.x) which detects consumer demand on the remote broker and only forwards messages as needed.
*   The demand based forwarding bridge now take advantage of the [Consumer Priority](consumer-priority) to avoid forwarding messages to a remote broker if there is a local consumer consuming it's messages.
*   Message fragmentation is no longer done. Fragmented messages add yet another level of complexity when you introduce broker networks. Large objects/streams should be transfered using the [JMS Streams](jms-streams).
*   JMS clients marshall fewer messages on the wire on for a rollback.
*   JMS clients marshall fewer messages when sessions/consumers/producers are closed.
*   The client and the broker make more extensive use of Thread Pools to avoid allocating idle threads that are not being used.

