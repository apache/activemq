---
title: "How lightweight is sending a message"
slug: /how-lightweight-is-sending-a-message
---

### For ActiveMQ 3.x/4.x

It depends.

If you are in a JMS transaction, are using non-durable messaging then its fairly lightweight and fast - typically just blocking until the message has got onto the socket buffer. Though if you are using durable messaging and not using JMS transactions then by default we will perform a blocking request-response with the broker to ensure the message is persisted to disk by the time the call to send() is complete - which is pretty slow.

However if you really want it to be very lightweight and fast, [enable async sending](configuring-transports) on your JMS connection.

If you really want low latency, such as in a GUI thread or very high performance server, you probably want to enable asynchronous sending. The only downside of asynchronous sending is if the send fails for whatever reason (security exception typically or some transport failure), then you don't get an exception thrown in the sender thread, since all the work is done asynchronously, though your ErrorListener will get notified.

(If you use the reliable transport, then you can let the JMS client handle transport errors and drop & reconnect to another broker to handle auto-reconnection for you).

