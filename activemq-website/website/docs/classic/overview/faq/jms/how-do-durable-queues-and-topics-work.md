---
title: "How do durable queues and topics work"
slug: /how-do-durable-queues-and-topics-work
---

Durable queues keep messages around persistently for any suitable consumer to consume them. Durable queues do not need to concern themselves with which consumer is going to consume the messages at some point in the future. There is just one copy of a message that any consumer in the future can consume.

Durable topics however are different as they must logically persist an instance of each suitable message for every durable consumer - since each durable consumer gets their own copy of the message.

For example imagine a durable subscriber S starts up subscribing to topic T at time D1. Some publisher sends messages M1, M2, M3 to the topic and S will receive each of these messages. Then S is stopped and the publisher continues to send M4, M5.

When S restarts at D2, the publisher sends M6 and M7. Now S will receive M4, M5 followed by M6 and M7 and all future messages. i.e. S will receive all messages from M1..M7.

This is the difference between durable and non-durable consuming. If S were a non-durable consumer then it would only have received M1, M2, M3 and M6, M7 - not M4 and M5. i.e. because the subscription is durable, S will receive every message sent to T whether the subscriber is running or not. For non-durable topics, only messages delivered to the topic T when S is running are delivered.

So for durable topic subscription, the JMS provider needs to be able to identify S when it shuts down and later on in the future reconnects, so it can know what messages to send to it while it was not running. JMS specification dictates that the identification of S is done by a combination of the clientID and the durable subscriber name. This is so that the JMS connection S uses can have many different durable subscriptions on different topics or on the same topic with different selectors - yet the JMS provider can know which message for which subscription to keep around for it.

So setting the clientID on a JMS connection is vital (along with using a sensible durable consumer name) for durable topic subscription. Its not an issue for other [QoS](qos)

