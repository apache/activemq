---
title: "How do I consume a specific message"
slug: /how-do-i-consume-a-specific-message
---

If you want to consume messages in a different order, or consume specific messages at the head, middle or tail of the queue, you can

*   browse the messages using the QueueBrowser to find the JMSMessageID's of the messages you want to consume
*   create a new consumer with a selector matching the ID(s) you want.

e.g. here is an example selector
```
JMSMessageID = 'abc'
```
Note that this is not a very efficient way of working with JMS (JMS is designed for consumers to be long lived objects working across many messageS), but it can be useful in certain situations.

Another option is just to use [JMX](jmx) directly to browse messages on a queue, process them and then delete them.

