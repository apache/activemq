---
title: "Wire Protocol"
slug: /wire-protocol
---


This page describes the logical [OpenWire](openwire) protocol for users developing clients in other languages than Java such as C# or C native clients. Note that OpenWire is designed for maximum performance and features; its the protocol used inside ActiveMQ. If you want a simpler protocol to work with to get started with a cross language client then try [Stomp](stomp) which is designed for ease-of-implementation so its easy to support many clients.

Protocol overview
=================

A client of ActiveMQ will exchange Command objects (following the _Command Pattern_). We'll describe how these commands are exchanged...

The protocol uses mostly one way messaging (fire and forget) for most kinds of commands - but there are times when RPCs (request, response) messaging is used. We'll indiciate the RPC scenarios clearly.

Establishing connections
------------------------

The client must send a ConnectionInfo command with details of the machine, host name, user name, password and most importantly the unique clientId the client wishes to use and then wait for a valid Response before continuing. The clientId must be generated using a unique string generator.

Sending messages
----------------

Once you've established a connection, you need to create a logical MessageProducer. This involves sending a ProducerInfo command with a unique producerId, sessionId and the clientId (connection ID).

From that point on you can send a Message command any time you like, provided you adorn the message with the clientId, sessionId, producerId along with a transactionId if required (see below on transactions).

Consuming messages
------------------

Commands are delivered on your inbound socket for consumption. Some of these will be Responses of previous RPCs, the rest will be incoming Messages for dispatch.

To start consuming messages you must send a ConsumerInfo command with details of the clientId, sessionId and a unique consumerId. From this point on you will receive inbound messages which contain the consumerId for which they are destined for.

To acknowledge a message send a MessageAck to the server (one way). Fill in the MessageAck with details of the consumerId, sessionId, clientId and any transactionIds.

Working with Responses
----------------------

When you create a connection, a producer and a consumer you will receive a Response to indicate the status of the request (such as did it work or fail). Since the protocol is asynchronous you can receive Response commands out of order; you could have multiple threads using the same connection/socket sending commands in parallel.

So its usual in multi-threaded clients to have some kind of correlation Map to match Command ids with Response Ids so that you can know which operation in what thread worked or failed etc. See the Transport implementations in Java to see how we do that. e.g. see [ResponseCorrelator](https://github.com/apache/activemq/tree/main/activemq-core/src/main/java/org/apache/activemq/transport/ResponseCorrelator.java)

IDs
---

It is currently a requirement of the client to automatically generate client, session, producer and consumer IDs which are typically Strings and indended to be globally unique. Similarly each Command has its own integer counter ID which is used for the client to correlate requests to responses; the client will typically use an incrementing rolling counter for these.

Closing producers, consumers, sessions, connections
---------------------------------------------------

To close a resource send a RemoveInfo command with the correct objectId for the producer, consumer, session, connection etc.

JMS Transactions
----------------

Each transacted session generates a unique transaction ID which is passed along on any transaction message. To start, commit or rollback the client should send a TransactionInfo command oneway (no Response is generated).

After a commit or rollback a new transaction ID should be generated and a new start TransactionInfo sent.

Any messages sent or message acknowledgements made should also propogate the transaction Id.

XA Transactions
---------------

As above but using the XATransactionInfo. Also we pass an XAid rather than a transactionID.

Final thoughts
==============

If in doubt about a piece of the protocol, the easiest way to figure out what happens is to check the JMS client code. In particular check out the code of [ActiveMQSession](http://activemq.codehaus.org/maven/xref/org/activemq/ActiveMQSession.html) which contains pretty much all the above logic. Pay particular attention to calls to **syncSendCommand()** and **asyncSendCommand()** when Commands are sent to the Message Broker

