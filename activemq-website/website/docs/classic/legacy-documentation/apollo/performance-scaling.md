---
title: "Apollo Performance and Scaling"
slug: /performance-scaling
---

       

Performance and Scaling
=======================

Performance
-----------

Apollo's performance is awesome considering it is using Stomp, a text based protocol. See [the STOMP benchmark report](http://hiramchirino.com/stomp-benchmark/ec2-c1.xlarge/index.html) for detailed results.

Scaling Characteristics
-----------------------

There are many different extreme ways that a messaging system can be used. Some folks like to:

*   connect a large number of clients.
*   hold a large number of messages in their queues.

Apollo aims to support all those usage scenarios.

### Scaling the Number of Connected Clients

Apollo uses non blocking IO and a reactor thread model. This means that a running broker uses a constant number of threads no matter how many clients are connected to it.

### Scaling the Number of Queued Messages

Queues will swap messages out of memory when there are no consumers that are likely to need the message soon. Once a message is swapped, the queue will replace the message with a reference pointer. When the queue builds a large number (more than 10,000) of these swapped out reference pointers, they then get consolidated into a single “range entry” and the pointers are dropped.

When a consumer comes along that needs some of the swapped out messages, it will convert previously consolidated “range entries” to a set of reference pointers. The next reference pointers that the consumer is interested in get swapped back as regular messages. This allows Apollo's queues to hold millions of messages without much impact on JVM memory usage.