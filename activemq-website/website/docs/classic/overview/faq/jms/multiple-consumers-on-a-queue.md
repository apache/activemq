---
title: "Multiple consumers on a queue"
slug: /multiple-consumers-on-a-queue
---

If you want to consume concurrently from a queue, then you must use a different session for each consumer.

This is because you must have a session per thread. The JMS contract is that only 1 session is used by one thread at once - which if you're using consumers means that only 1 consumer can receive messages at once if using the same session. So if you want concurrent consumption of messages, you need to use different sessions for each consumer.

However its your call on how many connections you have. In a normal network based mode, sharing the same connection across sessions will essentially multiplex traffic across a single socket (and so reduce resource usage at the cost of more synchronization). So sometimes there's a need to use more connections, as typically using different sockets concurrently tends to be a bit faster than using a single one).

Incidentally in the JCA specifications in J2EE 1.4, Resource Adapters will typically create 1 connection for each session, so there's a 1-1 mapping between the two.

