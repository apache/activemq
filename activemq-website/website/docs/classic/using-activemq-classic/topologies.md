---
title: "Topologies"
description: "Legacy overview of ActiveMQ deployment topologies: in-VM vm:// transport, client-server over TCP or SSL, embedded brokers, multicast peer-to-peer and JXTA."
slug: /topologies
displayed_sidebar: docs
---


ActiveMQ supports a wide range of different deployment topologies as well as [protocols](uri-protocols) & wire formats. The following diagram shows a federated network of brokers with a few different kinds of topology.

![](/img/BrokerTopology-1.png)

Which topology you choose is up to you. We'll now describe a few of these protocols in a little more detail.

In VM
-----

A useful option when unit testing is to limit JMS communication to within a single JVM. For this use the protocol
```
vm://localhost
```
You can segment the VM protocol to different groups - e.g. if you want to have logically different JMS networks within the same JVM, you can group networks using different URIs. e.g.
```
vm://localhost/foo
```
This will ensure that different segments do not interfere with one another. Though typically we use unique topic and queue destinations so that all traffic can coexist happily on the same logical network.

Client-Server
-------------

This is probably the most efficient and fastest solution for large numbers of clients requiring a diverse range of communication options from publish / subscribe to queue based communication. Typically the clients will connect with a Message Broker using a protocol, typically TCP or SSL but this could be NIO or other protocols.

We can load balance clients across brokers and provide broker failover so that we have a logical cluster of brokers with [HA](ha).

e.g.
```
tcp://somehost:port
```
Or for SSL
```
ssl://somehost:port
```
You can use [Discovery](discovery) to find the brokers available that you can connect to which makes it easier to seamlessly connect to a cluster of brokers.

Embedded Broker
---------------

This is logically equivalent to Client-Server but some (or all) clients contain a locally embedded broker. So communcation between the client and server (broker) are all within the same JVM and so do not use real networking - though brokers may communicate with other brokers or clients connected to it.

This can avoid the extra hop required to go from producer to broker to consumer - which is a great optimisation for RMI / RPC style situations, where you want the performance benefits (reduced latency) of point to point networking but with the scalabilty of a flexible messaging fabric.

Embedded Brokers can also simplify deployment options as well, its one less process to run.

Another use case for embedded brokers is to provide store and forward isolation from each service - so that the remote brokers can fail quite happily without affecting the service with the embedded broker. e.g. the entire network could fail, but a service could continue publishing messages to its embedded broker.

You can find out how to [configure an embedded broker here](how-do-i-embed-a-broker-inside-a-connection)

Peer to Peer
------------

This allows peer based clusters to be created where there is no server - just clients connecting together.  
There are various ways to implement a peer to peer JMS network. One easy way is just to use a multicast transport for communication; then all nodes on the same multicast address will receive all messages and the local embedded message broker will route messages to the necessary MessageConsumers.

We currently have 3 choices for multicast protocols

*   multicast
*   jgroups: uses the JGroups library to implement reliable multicast
*   jrms: uses Sun's JRMS library to implement reliable multicast

Multicast is great in development though often you might want to disable this feature in production and have well known servers fixed on specific machines. Often socket based communication (using pointcast) is much faster & better for heavy-lifting - particularly on Java - so we tend to recommend to use multicast mostly for discovery and use TCP / SSL for your heavy duty messaging.

Often we can use the peer to peer topology as a bootstrap to create a cluster of clients & brokers and then autodeploy servers into the cluster for a true grid style network.

So you can get the effect of a peer based network using [Discovery](discovery) together with either stand alone Brokers or using embedded brokers.

### JXTA

We have a JXTA transport which will use the full JXTA stack for negotiating NAT and across firewalls and so forth for creating a true peer based JMS network.
```
jxta://hostname:port
```
Currently you need to run one server which everyone connects to via JXTA. We've not yet created a pure peer network with JXTA

