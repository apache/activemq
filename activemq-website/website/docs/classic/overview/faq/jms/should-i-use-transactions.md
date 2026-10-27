---
title: "Should I use transactions"
slug: /should-i-use-transactions
---

There are four main approaches as to a client can consume messages. They are:

1.  Auto-acknowledgement
2.  Explicit acknowledgement via `Message.acknowledge()`
3.  JMS Transactions
4.  XA

For a discussion on XA see: [Should I use XA](should-i-use-xa)

The main difference between 1 & 2 and 3 & 4 is the latter allow messages to be rolled back and redelivered if an error occurs whilst processing. There is no JMS 'unacknowledge'. So for this reason JMS transactions should be preferred to message acknowledgements in most use cases.

It's a common misconception that transactions are inherently slow. In reality there's little difference in performance, from the broker's perspective, between the use of a JMS transaction and invoking `Message.acknowledge()`. However, a performance penalty is incurred when the delivery mode is set to  persistent. This causes the broker to block until the commit is synchronized to disk. This is similar to how `Message.acknowledge()` blocks when using a non-transactional JMS session.

Transaction support is also available for non-persistent delivery mode.

Batched Transactions Are The Fastest Way To Process Messages!

It's worth noting that the fastest way to consume persistent messages is to use a JMS transaction combined with message batching, e.g., have the commit boundary encompass multiple messages, not just one. This applies to both producers and consumers and to clients that are both.

When using transactions a batch of 1000 messages, say, can be sent in a single atomic step. The message transmission is asynchronous and therefore very fast. The producer need only perform one commit every batch in order to minimize the latency that would otherwise be incurred with disk syncing.

