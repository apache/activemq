---
title: "How do I configure distributed queues or topics"
slug: /how-do-i-configure-distributed-queues-or-topics
---

How do I configure distributed queues or topics
-----------------------------------------------

You don't need to explicitly [configure distributed queues or topics](how-do-i-configure-the-queues-i-want) as any queue or topic is automatically distributed across other brokers when the brokers are configured in either a store and forward network or a master/slave cluster.

So you just need to connect brokers together to form either

*   a [Store and Forward Network of Brokers](networks-of-brokers), which means the messages travel from broker to broker until they reach a consumer; with each message being owned by a single broker at any point in time
*   a [Master/Slave Cluster](masterslave), which means all messages are replicated across each broker in the master/slave cluster

### Also see

*   [How do I configure the queues I want](how-do-i-configure-the-queues-i-want)
*   [How do distributed queues work](how-do-distributed-queues-work)
*   [Networks of Brokers](networks-of-brokers)
*   [MasterSlave](masterslave)

