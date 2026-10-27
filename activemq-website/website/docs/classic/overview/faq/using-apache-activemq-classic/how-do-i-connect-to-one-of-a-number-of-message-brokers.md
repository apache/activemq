---
title: "How do I connect to one of a number of message brokers"
slug: /how-do-i-connect-to-one-of-a-number-of-message-brokers
---

You can specify a list of URLs to connect to (for example if you have message brokers running on a number of machines). To specify a list of URLs, use a comma separated list of URLs with a prefix of list:. e.g.
```
list:tcp://localhost:61699,tcp://localhost:61617,tcp://localhost:61698
```
The JMS client will then try and connect to each one in turn, in a random order until one is connected. If they all fail to connect a sleep occurs and then this loop is retried a number of times until either a connection can be established or an exception is thrown. For more detail see [Configuring Transports](configuring-transports)

