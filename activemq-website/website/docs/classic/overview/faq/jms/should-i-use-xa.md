---
title: "Should I use XA"
slug: /should-i-use-xa
---

Should I use XA transactions (two phase commit?)
------------------------------------------------

A common use of JMS is to consume messages from a queue or topic, process them using a database or EJB, then acknowledge / commit the message.

If you are using more than one resource; e.g. reading a JMS message and writing to a database, you really should use XA - its purpose is to provide atomic transactions for multiple transactional resources. For example there is a small window from when you complete updating the database and your changes are committed up to the point at which you commit/acknowledge the message; if there is a network/hardware/process failure inside that window, the message will be redelivered and you may end up processing duplicates.

The problem with XA is it can be a bit slow; as the XA protocol requires multiple syncs to disk to ensure it can always recover properly under every possible failure scenario. This adds significant cost (in terms of latency, performance, resources and complexity). Also quite a few EJB servers and databases don't actually properly support XA!

Be Careful

ActiveMQ does not currently support XA Transaction suspend / resume semantics.

### An alternative to XA

So a good optimisation is to use regular JMS transactions - with no XA - and just perform some duplicate message detection in your code to check you have not already processed the message.

For example you can use an [Idempotent Consumer](http://activemq.apache.org/camel/idempotent-consumer.html) from the [Enterprise Integration Patterns](enterprise-integration-patterns) to solve this scenario.

Or in pseudocode you could use something like the following...
```
onMessage
try {
  if I have not processed this message successfully before {
    do some stuff in the database / with EJBs etc
    jdbc.commit() (unless auto-commit is enabled on the JDBC)
  }
  jms.commit()
} catch (Exception e) {
  jms.rollback()
}
```
This leads to **much** better performance since you are not performing many slow syncs to disk (per transaction!) for the XA protocol. The only downside with this approach is it means you have to use some application specific logic to detect if you've processed the message before or not. However its quite common that you'll have some way of detecting this. e.g. if the message contains a purchase order and version; have you stored that purchase order and version in the database yet?

So provided the message has some kind of ID and version, you can often detect duplicates yourself and so not require to pay the performance cost of XA

It may also be worth reading [Should I use transactions](should-i-use-transactions).

### Further optimisation

Performing duplicate detection can add a performance overhead. e.g. doing a database query to see if you've processed the purchase order before etc. Most of the time things will work; its only in rare circumstances (ilke if a server crashes mid-way processing some messages) that messages get redelivered.

So you can use the [Message.getJMSRedelivered()](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/Message.html#getJMSRedelivered()) method to detect if a message has been redelivered and only if its been redelivered then perform the duplicate detection check.

