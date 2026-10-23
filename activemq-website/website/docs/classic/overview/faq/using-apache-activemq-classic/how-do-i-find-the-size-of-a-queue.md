---
title: "How do I find the Size of a Queue"
slug: /how-do-i-find-the-size-of-a-queue
---

How do I check on the size of the queue? I know it's not JMS standard, but most JMS implementations have a way to do this.

You can view the queue depth using the MBeans in ActiveMQ 5.x. Use any JMX management console to see the statistics. See [How can I monitor ActiveMQ](how-can-i-monitor-activemq-classic).

You can also browse the contents of a queue using the JMS [QueueBrowser](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/QueueBrowser.html).

Or you can access statistics [programmatically](how-can-i-see-what-destinations-are-used)

Through the MBeans, you can monitor individual destinations including message statistics related to the destination. For example, you'll see the following attributes on a destination (Queue or Topic):

*   Enqueue Count - the total number of messages sent to the queue since the last restart
*   Dequeue Count - the total number of messages removed from the queue (ack'd by consumer) since last restart
*   Inflight Count - the number of messages sent to a consumer session and have not received an ack
*   Dispatch Count - the total number of messages sent to consumer sessions (Dequeue + Inflight)
*   Expired Count - the number of messages that were not delivered because they were expired

The "size of a queue" is also explicitly called out in an attribute "QueueSize." QueueSize is the total number of messages in the queue/store that have not been ack'd by a consumer. This can become confusing at times when compared to the Enqueue Count because the Enqueue Count is a count over a period of time (since the last broker restart) while the Queue Size is not dependent on a period of time but instead on the actual number of messages in the store.

