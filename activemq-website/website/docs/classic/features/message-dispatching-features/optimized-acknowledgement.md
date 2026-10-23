---
title: "Optimized Acknowledgement"
slug: /optimized-acknowledgement
---


Overview
--------

ActiveMQ supports acknowledging a range of messages in a single batch operations. This option is disabled by default but can be used to improve throughput in some circumstances as it decreases load on the broker. Consider enabling it for your performance test scenario to see if it is applicable.

### Enabling Optimized Acknowledgements using a Connection URI
```
cf = new ActiveMQConnectionFactory("tcp://locahost:61616?jms.optimizeAcknowledge=true");
```

### Enabling Optimized Acknowledgements at the ConnectionFactory Level
```
((ActiveMQConnectionFactory)connectionFactory).setOptimizeAcknowledge(true);
```

### Enabling Optimized Acknowledgements at the Connection Level
```
((ActiveMQConnection)connection).setOptimizeAcknowledge(true);
```

### setOptimizeAcknowledgeTimeOut (5.6)

Since 5.4.2 there is a default timeout on a batch optimized acknowledge which ensures that acks are timely even if consumers are slow. On slow networks, the timeout can expire before the batch limit is reached so the reduced bandwith utilisation is bypassed. In version 5.6, the timeout is configurable via the optimizeAcknowledgeTimeOut attribute. Set as above via the connectiion URI or at the factory and connection level. The default value is 300ms, a value of 0 disables.

