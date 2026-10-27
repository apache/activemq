---
title: "How do distributed queues work"
slug: /how-do-distributed-queues-work
---

There are various [Topologies](topologies) that you can employ with ActiveMQ, where clients are connected to message brokers in various ways like

*   peer based
*   client server
*   hub and spoke

Each client communicates with a broker using some kind of [client library and network protocol](cross-language-clients).

To create distributed queues or topics we need to have the message brokers communicate with each other. There are two different types of broker to broker communication...

Master/Slave for High Availability
----------------------------------

A [Master/Slave Cluster](masterslave) is used for [HA](ha). Basically it means that all messages are replicated across each broker in the master/slave cluster. If the Master goes down, the clients can automatically failover to a slave which will have all the messages already, so each message is highly available. The Slave(s) provide a hot standby broker which will always be in sync ready to take over if the master goes away due to hardware failure etc.

Master/Slave works by having some form of replication; each message is owned by every broker in the logical cluster. A master/slave cluster then acts as one logical message broker which could then be connected via store and forward to other brokers (as we'll see in the next section).

### Distributed Queues and Topics

In Master/Slave, queues and topics are all replicated between each broker in the cluster (so often to a master and maybe a single slave). So each broker in the cluster has exactly the same messages available at any time so if a master fails, clients failover to a slave and you don't loose a message.

Store and forward networks of brokers
-------------------------------------

A [Store and Forward Network of Brokers](networks-of-brokers) means the messages travel from broker to broker until they reach a consumer; with each message being owned by a single broker at any point in time. When a JMS producer sends a message to a JMS consumer, it may travel through several brokers to reach its final destination. ActiveMQ uses [Consumer Priority](consumer-priority) so that local JMS consumers are always higher priority than remote brokers in a store and forward network.

Note though that a store and forward network is not a solution for message [HA](ha); if a broker fails in a Store and Forward network, the messages owned by that broker remain inside the broker's persistent store until the broker comes back online. If you need [HA](ha) of messages then you need to use Master/Slave described above.

Store and forward is often used in large networks where producers are on one LAN and consumers are on another LAN and you wish to use a broker on each LAN as a kind of network concentrator to minimise chattiness over the WAN between them (and to minimise the number of connections required across the WAN too). Similar uses of store and forward can be found when using firewalls or SSL across certain networks etc. One other use case for store and forward networks is if your OS does not support many sockets (and you can't reconfigure that) you could use a store and forward network to connect massive numbers of clients together in one logical network.

### Distributed Queues in Store/Forward

When we publish a message on a queue, it is stored in the persistent store of the broker that the publisher is communicating. Then if that broker is configured to store/foward to other brokers and clients, the broker will send it to _one_ of these clients (which could be a node or a broker depending on the dispatch algorithm). This dispatch algorithm continues until the message is finally dispatched and consumed by a client.

At any point in time the message will only exist in one broker's store until its consumed. Note that messages are only distributed onto other brokers if there is a consumer on those brokers.

e.g. if we had broker A, B, C and a publisher on a queue on A. If we have consumers on the queue on A and B then messages for the queue will be spread across both brokers A and B; some messages going to B, some being consumed on A, none going to C. If a consumer on the queue starts on C, then messages will flow there too. If the consumer stops then no more messages will be dispatched to C.

Distributed Topics in Store/Forward
-----------------------------------

For topics the above algorithm is followed except, every interested client receives a copy of the message - plus ActiveMQ will check for loops (to avoid a message flowing infinitely around a ring of brokers).

### See Also

*   [How do I configure distributed queues or topics](how-do-i-configure-distributed-queues-or-topics)
*   [MasterSlave](masterslave)
*   [Networks of Brokers](networks-of-brokers)

