---
title: "How can I monitor the connection with the broker"
slug: /how-can-i-monitor-the-connection-with-the-broker
---

How can I monitor the connection with the broker
------------------------------------------------

You can monitor the status of the connection with the broker via the [addTransportListener() method on the ActiveMQConnection](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/ActiveMQConnection.html#addTransportListener%28org.apache.activemq.transport.TransportListener).

This method takes a [TransportListener](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/transport/TransportListener.html) which is notified as the connection is established & dropped. It also allows you to work with the [Failover Transport](failover-transport-reference) yet still know the status of the connection.

### See Also

*   [How can I monitor ActiveMQ](how-can-i-monitor-activemq-classic)

