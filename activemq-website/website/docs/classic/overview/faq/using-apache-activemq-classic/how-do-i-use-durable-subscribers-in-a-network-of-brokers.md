---
title: "How do I use durable subscribers in a network of brokers"
slug: /how-do-i-use-durable-subscribers-in-a-network-of-brokers
---

Durable subscribers behave a little differently across a cluster of ActiveMQ brokers. The two main issues surround messages getting stuck on other brokers in the network after a durable subscriber has disconnected and reconnected to a different broker in the network without fully unsubscribing. The reasons for disconnecting/reconnecting can be voluntary (where the subscriber is using the failover transport), or involuntary (one of the brokers in the cluster fails).

For example, if you have two brokers A and B networked together in both directions to form a cluster, and a durable subscriber connects to broker B, a producer P on broker A will have its messages properly forwarded to broker B and the durable subscriber. However, if the subscriber disconnects and reconnects to broker A, any messages sent by P while the subscriber was away will be stuck on B until the subscriber reconnects to B.

The solution is to use [Virtual Destinations](virtual-destinations)

Virtual topics use queues under the covers and this allows ActiveMQ to treat each subscriber as a plain old Queue subscriber. This allows ActiveMQ to replay messages that have been orphaned on another broker in the cluster using this configuration as explained in the [Networks of Brokers](networks-of-brokers) documentation:
```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue="TEST.>" enableAudit="false">
        <conditionalNetworkBridgeFilterFactory replayWhenNoConsumers="true"/>
      </policyEntry>
    </policyEntries>
  </policyMap>
</destinationPolicy>
```
Also, take a look at [this unit test to see the full configuration and usage](http://svn.apache.org/viewvc/activemq/trunk/activemq-unit-tests/src/test/java/org/apache/activemq/usecases/VirtualTopicNetworkClusterReactivationTest.java?view=markup)

