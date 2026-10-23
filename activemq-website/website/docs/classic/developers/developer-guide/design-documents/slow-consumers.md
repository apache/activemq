---
title: "Slow Consumers"
slug: /slow-consumers
---


Blocked Transport
-----------------

when using TCP there can be occasions when a network outage can result in a blocked write. This can cause the entire broker to freeze - and the socket may never be unblocked. Currently we have a Thread that checks for blocked sockets - using a sweep protocol to detect if there are connections that are blocked writing for more than a configurable period. This can work but there is no way to unblock the calling thread that is associated with the send down the socket (which will be the publishing thread in non-durable topics).

Need to check if closing out the socket unblock the send.

TCP transports also use the InactivityMontor class as TransportFilter which detects dead sockets by forcing KeepAliveInfo commands periodically through the transport when it is idle. Therefore, the InactivityMontor can assume that if packets are not being received periodically, it means that the transport is dead and transport exception is generated.

Blocked Consumer
----------------

This is slightly different from above - The Consumer is blocked or very slow in processing a message. On the Client side the limit to how many messages a connection can hold is limited by the pre-fetch (which for non-durable topics is in the thousands).

Background on Slow Consumers
----------------------------

Slow consumers can cause problems in the broker. Here's the various things we can do.

In general slow consumers don't affect queues that much as consumers compete for messages; so a slow consumer just gets less than the others.

### Non-Durable Topics

Non-durable topics are the scenario which is most affected by slow consumers since the messages are not persistent and messages generally go to all consumers (who have a valid selector)

Here are the various things we can do

*   block/slow the producer
*   drop the slow consumer
*   spool messages to disk
*   discard messages for the slow consumer

These will be exposed as a pluggable policy for the user. It might be worth doing one on a destination by destination basis?

### Durable Topics

We can drop messages from RAM since they are persistent so we can deal with slow consumers well (assuming you have enough disk).  
If consumers get too far behind we could consider killing consumers; but I think thats more of a background operator issue?

### Durable queues

Since all messages are persisted, they can be evicted from memory.

### Non-durable queues

A slow consumer is not really an issue with queues. But all of the consumers being slow is. In this case we eventually block the producer until messages are consumed.

Other options could be to

*   spool messages to disk
*   discard messages

Implementation Solutions
------------------------

For persistent messages: Introduce a different dispatching model where we have a thread per destination with it's own memory allocation. This would allow us more control over dispatching, and allow us to set different priorities to different destinations.

For non-persistent messages - it's important to introduce an optional level of indirection between the producer broker thread and the write to the consumer's socket. This will allow us to plug-in writing to disk, throwing away messages and kill a blocked socket without affecting any other connections in the broker.

Our current default is to block producers until the slow consumer catches up (for non-durable topics here).

Another option that should be possible is, if a consumer is marked as a _slow consumer_ then we can discard messages being delivered to it until it stops being a slow consumer. This should be fairly easy to do if we have a way of marking a Subscription object as being slow.

More advanced variants will be introduced over time. This may include:

*   Only activating a slow consumer policy based on the percentage of slow consumers - e.g. if all the consumers are slow, you may wish to block the publisher, but if onlt one or two are slow, you may wish to take some action
*   Closing a slow consumer
*   Writing a finite amount of messages to disk
*   customized discard polices - you may wish to discard messages based on pre-set patterns or selectors
*   Combinations of the above ...

### Slow Consumer Detector

We need a good way of detecting that a consumer is slow - together with knowing when the consumer speeds up again

