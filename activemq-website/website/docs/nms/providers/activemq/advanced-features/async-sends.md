---
title: "NMS.ActiveMQ Async Sends"
---

Background
----------

Apache.NMS.ActiveMQ supports sending messages to a broker in sync or async mode. The mode used has a huge impact in the latency of the send call. Since latency is typically a huge factor in the throughput that can achieved by producer, using async sends can increase the performance of your system dramatically.

The good news is that Apache.NMS.ActiveMQ sends message in async mode by default in several cases. It is only in cases where the JMS specification required the use of sync sending that we default to sync sending. The cases that we are forced to send in sync mode are when persistent messages are being sent outside of a transaction.

If you are not using transactions and are sending persistent messages, then each send is synch and blocks until the broker has sent back an acknowledgement to the producer that the message has been safely persisted to disk. This ack provides that guarantee that the message will not be lost but it also costs a huge latency penalty since the client is blocked.

Many high performance applications are designed to tolerate a small amount of message loss in failure scenarios. If your application has been designed in this fashion, you can enable the use of async sends to increase throughput even when using persistent messages.

### Configuring Async Send using a Connection URI

You can use the [Connection Configuration URI](/components/nms/providers/activemq/uri-configuration) to configure async sends as follows
```
var cf = new ConnectionFactory("tcp://locahost:61616?nms.AsyncSend=true");
```
### Configuring Async Send at the ConnectionFactory Level

You can enable this feature on the Apache.NMS.ActiveMQ.ConnectionFactory object using the property.
```
((ConnectionFactory)connectionFactory).AsyncSend = true;
```
### Configuring Async Dispatch at the Connection Level

Configuring the async send setting at this level overrides the settings at the connection factory level.

You can enable this feature on the Apache.NMS.ActiveMQ.Connection object using the property.

((Apache.NMS.ActiveMQ.Connection)connection).AsyncSend = true;

### Also see

*   [Connection Configuration URI](/components/nms/providers/activemq/uri-configuration)
*   [Should I use transactions](http://activemq.apache.orgCommunity/FAQ/JMSCommunity/FAQ/JMS/Community/FAQ/JMS/should-i-use-transactions.md)
*   [Consumer Dispatch Async](/components/nms/providers/activemq/advanced-features/consumer-dispatch-async)
