---
title: "Apollo 1.7.1 OpenWire Protocol Manual"
slug: /openwire-manual
---

       

Apollo 1.7.1 OpenWire Protocol Manual
=====================================

*   [Using the OpenWire Protocol](#using-the-openwire-protocol)
*   *   [OpenWire Protocol Options](#openwire-protocol-options)
    *   [Protocol Detection (different that open-wire vesion detection)](#protocol-detection-different-that-open-wire-vesion-detection)
    *   [Client Libraries](#client-libraries)
    *   [Broker features available using the OpenWire protocol](#broker-features-available-using-the-openwire-protocol)
    *   *   [Destination Types](#destination-types)
        *   [Wildcard Subscriptions](#wildcard-subscriptions)
        *   [Composite Destinations](#composite-destinations)
        *   [Exclusive Consumer](#exclusive-consumer)
        *   [Temporary Destinations](#temporary-destinations)
        *   [Message Selectors](#message-selectors)
        *   [Browing Subscription](#browing-subscription)
        *   [Transactions](#transactions)
    *   [OpenWire protocol details](#openwire-protocol-details)
    *   [OpenWire features to be documented](#openwire-features-to-be-documented)
    *   [Unsupported OpenWire features:](#unsupported-openwire-features)

Using the OpenWire Protocol
---------------------------

Clients can connect to Apollo using the [OpenWire](http://activemq.apache.orgConnectivity/Protocols/openwire) protocol. OpenWire is a binary, on-the-wire protocol used natively by [ActiveMQ](http://activemq.apache.org/). It was designed to be a fast, full-featured, and JMS-compliant protocol for message brokers. Currently there are native client libraries for Java, C, C#, and C++. Further OpenWire support can be built by implementing language-specific code generators, however, for most cross-langauge needs, the [STOMP](http://stomp.github.com) protocol is best.

OpenWire was designed to be extended but yet backward compatible with older versions. When a client connects to the broker, the protocol version that's used is negotiated based on what each can support.

### OpenWire Protocol Options

You can use the `openwire` configuration element within the `connector` element in the `apollo.xml` configuration file to change the default settings used in the OpenWire protocol implementation.

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <openwire attribute="value"/>
    </connector>
    

The `openwire` element supports the following configuration attributes:

*   `buffer_size` : How much each producer or subscription will buffer between the client and the broker. Defaults to `640k`.
*   `stack_trace` : If there is an exception on the broker, it will be sent back to the client. Default is `true`
*   `cache` : Used to reduce marshalling efforts within the broker. Cache data structures such as openwire commands, destination objects, subscription info, etc. Default is `true`
*   `cache_size` : Number of internal data structures to cache. Default is `1024`
*   `tight_endcoding` : Optimize the encoding to be effecient over the wire at the expense of greater CPU usage to marshal/unmarshal. Default is `true`
*   `tcp_no_delay` : Decide whether to use [Nagle's Algorithm](http://en.wikipedia.org/wiki/Nagle's_algorithm) which improves TCP/IP effeciency for small packets. Set to true to disable this algorithm. Default is `false` (which means nodelay is off, and it uses Nagle's algorithm)
*   `max_inactivity_duration` : Max inactivity period, in milliseconds, at which point the socket would be considered dead. Used by the heartbeat functionality. If there is a period of inactivity greater than this period, the socket will be closed. Default is `30000`
*   `max_inactivity_duration_initial_delay` : Amount of time to delay between determining the socket should be closed and actually closing it. Default is `30000`
*   `max_frame_size` : Size in bytes of the largest frame that can be sent to the broker. Default is `100MB`
*   `add_jmsxuserid` : If set to `false`, disables setting the JMSXUserID header on received messages. Default is `true`.

An example of configuring the OpenWire protocol

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <openwire tight_encoding="false" tcp_no_delay="true"/>
    </connector>
    

### Protocol Detection (different that open-wire vesion detection)

Apollo was designed to be inherently multi-protocol. Although STOMP was the first protocol to be implemented in Apollo, the core of the broker was not built around STOMP or any other specific protocol. Apollo, in fact by default, has the ability to detect the protocol being used on the wire without further configuration. This makes the configuration easier on the broker, and means you only need to open one connector that can handle multiple different types of wire protocols. If you would like to specify a certain connector for OpenWire and another connector for a different protocol, you can explicitly configure the connector to be an OpenWire connector:

    
    <connector protocol="openwire" ... />
    

You can also support a limited subset of protocols:

    
    <connector bind="...">
        <detect protocols="openwire stomp" />
    </connector>
    

Or you can leave it open to any of the supported protocols (default), and the correct protocol will be used depending on what the client is using. You do this by not specifying any protocol settings.

Note, this type of on-the-wire protocol detection is different that the OpenWire version detection briefly mentioned above. After the broker determines a client is using an OpenWire protocol, the version is negotiated separately from how the broker determines a protocol.

### Client Libraries

To connect to Apollo using the OpenWire protocol, we recommend you use the latest [ActiveMQ](http://activemq.apache.org/components/classic/download) 5.x client libraries.

*   [C](http://activemq.apache.orgConnectivity/Cross Language ClientsConnectivity/Cross Language Clients/Connectivity/Cross Language Clients/c-integration)
*   [C++](http://activemq.apache.orgConnectivity/Cross Language ClientsConnectivity/Cross Language Clients/Connectivity/Cross Language Clients/activemq-c-clients)
*   [C# and .NET](http://activemq.apache.org/nms/)

To configure specific behaviors for your connection, see the [Connection reference](http://activemq.apache.org/components/classic/documentation/connection-configuration-uri) for ActiveMQ 5.x

### Broker features available using the OpenWire protocol

#### Destination Types

*   Queues (for point-to-point messaging) - A JMS Queue implements load balancer semantics. A single message will be received by exactly one consumer. If there are no consumers available at the time the message is sent it will be kept until a consumer is available that can process the message. If a consumer receives a message and does not acknowledge it before closing then the message will be redelivered to another consumer. A queue can have many consumers with messages load balanced across the available consumers.
    
*   Topics (publish-subscribe) - In JMS a Topic implements publish and subscribe semantics. When you publish a message it goes to all the subscribers who are interested - so zero to many subscribers will receive a copy of the message. Only subscribers who had an active subscription at the time the broker receives the message will get a copy of the message.
    
*   Durable Subscriptions (persistent publish-subscribe) - Durable subscriptions allow you to achieve semantics similar to a queue using topics. Specifically, this allows a subscription to subscribe and disconnect without worrying about losing messages. If the client disconnects, the messages that arrive at the topic while the subscription is inactive will be queued up for consumption when the subscription becomes reactivated.
    

#### Wildcard Subscriptions

Wild cards can be used in destination names when subscribing as a consumer. This allows you to subscribe to multiple destinations or hierarchy of destinations.

*   `.` is used to separate names in a path
*   `*` is used to match any name in a path
*   `>` is used to recursively match path names

Unlike some of the other protocols Apollo supports, for the OpenWire implementation, regex wildcards are not supported. Also note that for other protocols, the wildcard for recursive destinations is indeed “>" and not “**".

#### Composite Destinations

You can send to multiple destinations with one single operation. When you create a destination to which your producer will be sending, you can specify multiple destinations with the “," (comma) destination separator. For example, if you want to send a single message to two queues:

    Destination destination = session.createQueue("test-queue,test-queue-foo")
    MessageProducer producer = session.createProducer(destination);
    TextMessage message = session.createTextMessage("Message #" + i);
    producer.send(message);

Note both destinations named will be considered queues. However, you can also include a topic destination in your list. You'll want to use the `topic://` prefix if mixing destination types (or `queue://` for queues):

    Destination destination = session.createQueue("test-queue,test-queue-foo,topic://test-topic-foo")

Similarly you can consume from multiple destinations as well. When you set up your consumer's destination, just follow the same rules as above.

#### Exclusive Consumer

To do exclusive consumer on a queue, you will specify the settings on the queue itself:

    "QUEUE.NAME?consumer.exclusive=true"

The first consumer to subscribe to the queue will be the exclusive consumer. Any other consumers that subscribe to the queue will not receive messages as long as the exclusive consumer is alive and consuming. If the exclusive consumer goes away, the next in line to subscribe will be selected as the exclusive consumer. In general, the order that's calculcated for who should be the next exclusive consumer is based on when they subscribe. The first to subscribe wins and the others fall in line based on when they subscribed.

#### Temporary Destinations

Temporary destinations are bound to the connection that created them; therefore, when the connection goes away, the temporary destination will also go away. Using temporary is one way to implement a request-reply messaging pattern with Apollo. The steps for using temporary queues or topics for request-reply are as follows:

Create a temporary destination

    Destination replyDest = session.createTemporaryQueue();

Create a consumer for that destination

    MessageConsumer replyConsumer = session.createConsumer(replyDest);

Create a message to send as a request and set the JMSReplyTo header to the temp destination

    message.setJMSReplyTo(replyDest);

Send the message. If the receiver of the message is aware that it's participating in a request-reply scenario, it should place the response into the destination specified in the JMSReplyTo header.

#### Message Selectors

You can use message selectors to create subscriptions to destinations that are filtered based on some headers or properties in the message. You define a selector as a String that is similar to the SQL92 syntax.

For example, to define a consumer on a destination that is only interested in messages that have a property named “intended” and a value of “me”, pass a selector as the second argument to the [session.createConsumer()](http://docs.oracle.com/javaee/6/api/javax/jms/Session.html) method:

    session.createConsumer(destination, "intended = 'me'");

Now messages produced with a property/value combination specified in the selector will be delivered to the consumer.

Here's an example of producing the message:

    
    MessageProducer producer = session.createProducer(destination);
    
    for (int i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
        TextMessage message = session.createTextMessage("Message #" + i);
        LOG.info("Sending message #" + i);
        producer.send(message);
        Thread.sleep(DELAY);
    }
    

#### Browing Subscription

With a [QueueBrowser](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQQueueBrowser.html), you can browse a queue's messages without actually consuming them. This can be useful for debugging, adding a user-interface layer, or audit or logging.

To establish a browsing subscription to a queue, use the JMS API:

    QueueBrowser browser = session.createBrowser((Queue) destination);

Then you can enumerate the messages and examine them with the following idiom:

    Enumeration enumeration = browser.getEnumeration();
    
    while (enumeration.hasMoreElements()) {
        TextMessage message = (TextMessage) enumeration.nextElement();
        System.out.println("Browsing: " + message);
    }

When you browse a queue, only a snapshot of the queue will be available. If more messages are enqueued, the browsing session will not automatically see those.

Note, you cannot establish browsing sessions to a durable topic with OpenWire/JMS.

#### Transactions

Transactions can be done on both the consumer and the producer for any destination. When you create a session, pass `true` to the first parameter:

    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

You can `commit` or `rollback` a transaction by calling `session.commit()` or `session.rollback()` respectively. On the broker side, each command that you take before calling `session.commit()` (like sending a message) gets batched up in a TransactionContext. When commit is made, all of the commands are executed and a Response is sent to the client (i.e., calling commit is a synchronous call. Before calling commit, all other commands are asyc).

### OpenWire protocol details

This section explains a little more about what's happening on the wire. The STOMP protocol, as it was designed, is easy to understand and monitor since it's a text-based protocol. OpenWire, however, is binary, and understanding the interactions that happen isn't as easy. Some clues might be helpful.

All OpenWire commands are implemented as “command” objects following the Gang of Four [Command Pattern](http://en.wikipedia.org/wiki/Command_pattern). The structure of the objects are described [at the ActiveMQ website](http://activemq.apache.org/components/classic/documentation/openwire-version-2-specification), but what about the interactions?

Establishing a connection to the broker: A connection is established between the client and the broker with the client creating a new ActiveMQConnection (most likely using a connection factory of some sort). When a new “connection” is created, the underlying transport mechanisms send a WireFormatInfo command to the broker. This command describes what version and configurations of the OpenWire protocol the client wishes to use. For example, some of the configuration options are the ones listed above that can also be configured on the broker.

When the TCP connection is handled on the broker side, it sends a WireFormatInfo to the client. The purpose of exchanging these WireFormatInfo commands is to be able to negotiate what settings to use as each the client and the server has their own preferred settings. The lowest protocol version between the two is used. When the broker receives the client's WireFormatInfo command, it negotiates the differences on its side and then sends a BrokerInfo command. Conversely on the client, when it receives the broker's WireFormatInfo, it negotiates it and sends a ConnectionInfo command. When the broker receives a ConnectionInfo command, it will either ack it with a Response command, or use security settings established globally for the broker or for a given virtual host to determine whether connections are allowed. If a connection is not allowed to the broker or to to virtual host, the broker will kill the connection.

### OpenWire features to be documented

*   Flow Control
*   Persistent Messaging
*   Message Expiration

### Unsupported OpenWire features:

You will get bad/undefined behaviour if you try to use any of the following OpenWire features:

*   XA transactions
*   [Message Groups using JMSXGroupID](http://activemq.apache.org/Features/Consumer Features/message-groups)
*   [Subscription recovery/retroactive consumer](http://activemq.apache.org/Features/Consumer Features/retroactive-consumer)
*   [Exclusive Consumer with Priority](http://activemq.apache.orgFeatures/Consumer Features/exclusive-consumer)
*   [Virtual Destinations](/components/classic/documentation/virtual-destinations)

You can use Durable Subscriptions and/or [Mirrored Queues](user-manual.md#mirrored-queues) to get the same/similar behaviour that [Virtual Destinations](/components/classic/documentation/virtual-destinations) provide.