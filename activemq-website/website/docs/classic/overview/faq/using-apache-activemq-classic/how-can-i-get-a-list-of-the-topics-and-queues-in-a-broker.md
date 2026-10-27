---
title: "How can I get a list of the topics and queues in a broker"
slug: /how-can-i-get-a-list-of-the-topics-and-queues-in-a-broker
---

How can I get a list of the topics and queues in a broker?
----------------------------------------------------------

As of 5.1.0 you can use the new [DestinationSource](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/advisory/DestinationSource.html) on an ActiveMQConnection to access the available queues or topics or listen to queues/topics being created or deleted.

You can use [JMX](jmx) to browse the available topics and queues in a broker together with the connectors and connections along with their statistics etc.

Or you can access statistics [programmatically](how-can-i-see-what-destinations-are-used).

Finally you could just use the [Web Console](web-console)

