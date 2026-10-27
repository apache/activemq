---
title: "How do I make messages durable"
slug: /how-do-i-make-messages-durable
---

Durability of messages is defined by the [MessagerProducer](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/MessageProducer.html). You can explicitly configure the durability via the [setDeliveryMode() method](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/MessageProducer.html#setDeliveryMode(int)). e.g.
```
MessageProducer producer = ...;
producer.setDeliveryMode(DeliveryMode.PERSISTENT);
```
Note that the default is for durable messages to be used if you don't specify a delivery mode.

