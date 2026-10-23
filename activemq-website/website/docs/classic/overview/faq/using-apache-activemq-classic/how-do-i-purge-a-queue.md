---
title: "How do I purge a queue"
slug: /how-do-i-purge-a-queue
---

A frequent requirement is to purge a queue (i.e. delete all the messages on it).

### Solution

You can use the [Web Console](web-console) to view queues, add/remove queues, purge queues or delete/forward individual messages.

Another option is to use [JMX](jmx) to browse the queues and call the **purge()** method on the [QueueViewMBean](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/jmx/QueueViewMBean.html).

You could also delete the queue via removeQueue(String) or removeTopic(String) methods on the [BrokerViewMBean](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/jmx/BrokerViewMBean.html)

You can also do it [programmatically](how-can-i-see-what-destinations-are-used)

### Also See

*   [Web Console](web-console)
*   [JMX](jmx)
*   [How do I find the Size of a Queue](how-do-i-find-the-size-of-a-queue)
*   [How can I see what destinations are used](how-can-i-see-what-destinations-are-used)

