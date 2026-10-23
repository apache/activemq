---
title: "How do I enable asynchronous sending"
slug: /how-do-i-enable-asynchronous-sending
---

The default setting for ActiveMQ is that all persistent messages outside of a transaction are sent to a broker are synchronous. This means that the send method is blocked until the message is received by the broker, its then written to disk - then a response is returned to the client and the send() unblocks with success or throws a JMSException if the send could not complete (e.g. due to a security exception).  In the case of the persistent messages being sent in a transaction, only the commit is synchronous since if the commit succeeds, then it means that all of the sends and acknowledgements in the transaction succeeded.

For performance reasons you may wish to stream messages to the broker as fast as possible even if you are using persistent messages. So you can enable **asynchronous sending** of persistent messages using one of the following options

*   set the useAsyncSend property on the ActiveMQConnectionFactory
*   set the property using the URI when you connect to the broker

```
tcp://localhost:61616?jms.useAsyncSend=true
```