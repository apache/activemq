---
title: "How do I set the message expiration"
slug: /how-do-i-set-the-message-expiration
---

JMSExpiration on a message is set by the MessageProducer in JMS - either via

*   [producer.setTimeToLive()](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/MessageProducer.html#setTimeToLive(long))
*   [producer.send(Destination, Message, int, int, long)](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/MessageProducer.html#send(javax.jms.Destination,%20javax.jms.Message,%20int,%20int,%20long))

For the end-to-end view of how TTL becomes `JMSExpiration`, how the broker enforces expiry, and why clock synchronization matters, see [Message Expiry](message-expiry).
