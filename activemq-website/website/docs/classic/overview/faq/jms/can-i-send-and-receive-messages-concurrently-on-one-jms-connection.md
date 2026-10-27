---
title: "Can I send and receive messages concurrently on one JMS Connection"
slug: /can-i-send-and-receive-messages-concurrently-on-one-jms-connection
---

Absolutely!

Strictly speaking each producer being used concurrently should be using a separate session (though in ActiveMQ it'll probably work fine if you just use one session for all publishers).

For concurrent consumption create a session per consumer - as all messages are dispatched to a session in a single thread - but you can have as many sessions as you like per connection.

To further help with concurrent consuming of JMS you can use [Message Driven POJOs](http://jencks.codehaus.org/Message+Driven+POJOs)

