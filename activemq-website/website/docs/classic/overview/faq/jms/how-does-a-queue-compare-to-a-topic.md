---
title: "How does a Queue compare to a Topic"
slug: /how-does-a-queue-compare-to-a-topic
---

### Topics

In JMS a Topic implements _publish and subscribe_ semantics. When you publish a message it goes to all the subscribers who are interested - so zero to many subscribers will receive a copy of the message. Only subscribers who had an active subscription at the time the broker receives the message will get a copy of the message.

### Queues

A JMS Queue implements _load balancer_ semantics. A single message will be received by exactly one consumer. If there are no consumers available at the time the message is sent it will be kept until a consumer is available that can process the message. If a consumer receives a message and does not acknowledge it before closing then the message will be redelivered to another consumer. A queue can have many consumers with messages _load balanced_ across the available consumers.

So Queues implement a reliable load balancer in JMS.

