---
title: "How do I send messages to different Destinations from a single MessageProducer"
slug: /how-do-i-send-messages-to-different-destinations-from-a-single-messageproducer
---

How do I send messages to different Destinations from a single MessageProducer?
-------------------------------------------------------------------------------

Create the MessageProducer using a null destination; then specify the destination each time you send...
```
MessageProducer producer = session.createProducer(null);
...
producer.send(someDestination, message);
...
producer.send(anotherDestination, message);
```