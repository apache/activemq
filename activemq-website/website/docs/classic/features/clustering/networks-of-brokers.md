---
title: "Networks of Brokers"
slug: /networks-of-brokers
---


To provide massive scalability of a large messaging fabric you typically want to allow many brokers to be connected together into a network so that you can have as many clients as you wish all logically connected together - and running as many message brokers as you need based on your number of clients and network topology.

If you are using [client/server or hub/spoke style topology](topologies) then the broker you connect to becomes a single point of failure which is another reason for wanting a network (or cluster) of brokers so that you can survive failure of any particular broker, machine or subnet.

From 1.1 onwards of ActiveMQ supports _networks of brokers_ which allows us to support [distributed queues and topics](how-do-distributed-queues-work) across a network of brokers.

This allows a client to connect to any broker in the network - and fail over to another broker if there is a failure - providing from the clients perspective a [HA](ha) cluster of brokers.

**N.B.** By default a network connection is one way only - the broker that establishes the connection _passes messages to_ the broker(s) its connected to. From version 5.x of ActiveMQ, a network connection can be optionally enabled to be duplex, which can be useful for hub and spoke architectures, where the hub is behind a firewall etc.

Configuring a network of brokers
--------------------------------

The easiest way to configure a network of brokers is via the [Xml Configuration](xml-configuration). There are two main ways to create a network of brokers

*   use a hard coded list of networkConnector elements.

*   use Discovery to detect brokers (multicast or rendezvous).

### Example with a fixed list of URIs

Here is an example of using the fixed list of URIs
```
<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://activemq.org/config/1.0">

  <broker brokerName="receiver" persistent="false" useJmx="false">  
    <networkConnectors>
      <networkConnector uri="static:(tcp://localhost:62001)"/>
    </networkConnectors>

    <persistenceAdapter>
      <memoryPersistenceAdapter/>
    </persistenceAdapter>

   <transportConnectors>
      <transportConnector uri="tcp://localhost:62002"/>
    </transportConnectors>
  </broker>

</beans>
```
ActiveMQ also supports other transports than tcp to be used for the network connector such as http.

### Example using multicast discovery

This example uses [multicast discovery](discovery#multicast)
```
<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://activemq.org/config/1.0">

  <broker name="sender" persistent="false" useJmx="false">  
    <networkConnectors>
      <networkConnector uri="multicast://default"/>
    </networkConnectors>

    <persistenceAdapter>
      <memoryPersistenceAdapter/>
    </persistenceAdapter>

  <transportConnectors>
      <transportConnector uri="tcp://localhost:0" discoveryUri="multicast://default"/>
    </transportConnectors>
  </broker>

</beans>
```

Starting network connectors
---------------------------

By default, network connectors are initiated serially as part of the broker start up sequence. When some networks are slow, they prevent other networks from starting in a timely manner. Version 5.5 supports the broker attribute networkConnectorStartAsync="true" which will cause the broker to use an executor to start network connectors in parallel, asynchronous to a broker start.

Static discovery
----------------

With `static:` discovery you can hard code the list of broker URLs. A network connector will be created for each one.
```
<networkConnectors>
  <networkConnector uri="static:(tcp://host1:61616,tcp://host2:61616,tcp://..)"/>
</networkConnectors>
```
There are some useful properties you can set on a static network connector for retries:

property|default|description
---|---|---
initialReconnectDelay|1000|time(ms) to wait before attempting a reconnect (if useExponentialBackOff is false)
maxReconnectDelay|30000|time(ms) to wait before attempting to re-connect
useExponentialBackOff|true|increases time between reconnect for every failure in a reconnect sequence
backOffMultiplier|2|multipler used to increase the wait time if using exponential back off

e.g.
```
uri="static:(tcp://host1:61616,tcp://host2:61616)?maxReconnectDelay=5000&useExponentialBackOff=false"
```

MasterSlave Discovery
---------------------

A common configuration option for a network of brokers is to establish a network bridge between a broker and an n+1 broker pair (master/slave). Typical configurations involve using the `failover:` transport, but there are a some other non-intuitive options that must be configured for it to work as desired. For this reason, ActiveMQ v5.6+ has a convenience discovery agent that can be specified with the `masterslave:` transport prefix:
```
<networkConnectors>
  <networkConnector uri="masterslave:(tcp://host1:61616,tcp://host2:61616,tcp://..)"/>
</networkConnectors>
```
The URIs are listed in order for: MASTER,SLAVE1,SLAVE2...SLAVE(n)

The same configuration options for `static:` are available for `masterslave:`

NetworkConnector Properties
---------------------------

property|default|description
---|---|---
name|bridge|name of the network - for more than one network connector between the same two brokers - use different names
dynamicOnly|false|if true, only activate a networked durable subscription when a corresponding durable subscription reactivates, by default they are activated on startup.
decreaseNetworkConsumerPriority|true|if true, starting at priority -5, decrease the priority for dispatching to a network Queue consumer the further away it is (in network hops) from the producer. When false all network consumers use same default priority(0) as local consumers (before v5.18.0 default=false)
networkTTL|1|the number of brokers in the network that messages and subscriptions can pass through (sets both message&consumer -TTL)
messageTTL|1|(version 5.9) the number of brokers in the network that messages can pass through
consumerTTL|1|(version 5.9) the number of brokers in the network that subscriptions can pass through (keep to 1 in a mesh)
conduitSubscriptions|true|multiple consumers subscribing to the same destination are treated as one consumer by the network
excludedDestinations|empty|destinations matching this list won't be forwarded across the network (this only applies to dynamicallyIncludedDestinations)
dynamicallyIncludedDestinations|empty|destinations that match this list **will** be forwarded across the network **n.b.** an empty list means all destinations not in the exluded list will be forwarded
useVirtualDestSubs|false|if true, the network connection will listen to advisory messages for virtual destination consumers
staticallyIncludedDestinations|empty|destinations that match will always be passed across the network - even if no consumers have ever registered an interest
duplex|false|if true, a network connection will be used to both produce **_AND_** Consume messages. This is useful for hub and spoke scenarios when the hub is behind a firewall etc.
prefetchSize|1000|Sets the [prefetch size](what-is-the-prefetch-limit-for) on the network connector's consumer. It must be > 0 because network consumers do not poll for messages
suppressDuplicateQueueSubscriptions|false|(from 5.3) if true, duplicate subscriptions in the network that arise from network intermediaries will be suppressed. For example, given brokers A,B and C, networked via multicast discovery. A consumer on A will give rise to a networked consumer on B and C. In addition, C will network to B (based on the network consumer from A) and B will network to C. When true, the network bridges between C and B (being duplicates of their existing network subscriptions to A) will be suppressed. Reducing the routing choices in this way provides determinism when producers or consumers migrate across the network as the potential for dead routes (stuck messages) are eliminated. networkTTL needs to match or exceed the broker count to require this intervention.
bridgeTempDestinations|true|Whether to broadcast advisory messages for created temp destinations in the network of brokers or not. Temp destinations are typically created for request-reply messages. Broadcasting the information about temp destinations is turned on by default so that consumers of a request-reply message can be connected to another broker in the network and still send back the reply on the temporary destination specified in the JMSReplyTo header. In an application scenario where most/all messages use request-reply pattern, this will generate additional traffic on the broker network as every message typically sets a unique JMSReplyTo address (which causes a new temp destination to be created and broadcasted via an advisory message in the network of brokers). When disabling this feature such network traffic can be reduced but then producer and consumers of a request-reply message need to be connected to the same broker. Remote consumers (i.e. connected via another broker in your network) won't be able to send the reply message but instead raise a "temp destination does not exist" exception.
alwaysSyncSend|false|(version 5.6) When true, non persistent messages are sent to the remote broker using request/reply in place of a oneway. This setting treats both persistent and non-persistent messages the same.
staticBridge|false|(version 5.6) If set to true, broker will not dynamically respond to new consumers. It will only use `staticallyIncludedDestinations` to create demand subscriptions
userName|null|The username to authenticate against the remote broker
password|null|The password for the username to authenticate against the remote broker

#### Reliability

Networks of brokers do reliable store and forward of messages. If the source is durable, persistent messages on a queue or a durable topic subscription, a network will retain the durability guarantee.  
However networks cannot add durability when the source is non durable. Non durable topic subscriptions and temporary destinations (both queues and topics) are non durable by definition. When non durable  
sources are networked, in the event of a failure, inflight messages can be lost.

#### Ordering

Total message ordering is not preserved with networks of brokers. Total ordering [works with a single consumer](how-do-i-preserve-order-of-messages) but a networkBridge introduces a second consumer. In addition, network bridge consumers forward messages via producer.send(..), so they go from the head of the queue on the forwarding broker to the tail of the queue on the target. If single consumer moves between networked brokers, total order may be preserved if all messages always follow the consumer but this can be difficult to guarantee with large message backlogs.

#### When to use and not use Conduit subscriptions

ActiveMQ relies on information about active consumers (subscriptions) to pass messages around the network. A broker interprets a subscription from a remote (networked) broker in the same way as it would a subscription from a local client connection and routes a copy of any relevant message to each subscription. With Topic subscriptions and with more than one remote subscription, a remote broker would interpret each message copy as valid, so when it in turns routes the messages to its own local connections, duplicates would occur. Hence default conduit behavior consolidates all matching subscription information to prevent duplicates flowing around the network. With this default behaviour, N subscriptions on a remote broker look like a single subscription to the networked broker.

However - duplicate subscriptions is a useful feature to exploit if you are only using Queues. As the load balancing algorithm will attempt to share message load evenly, consumers across a network will equally share the message load only if the flag `conduitSubscriptions=false`. Here's an example. Suppose you have two brokers, A and B, that are connected to one another via a forwarding bridge. Connected to broker A, you have a consumer that subscribes to a queue called `Q.TEST`. Connected to broker B, you have two consumers that also subscribe to `Q.TEST`. All consumers have equal priority. Then you start a producer on broker A that writes 30 messages to `Q.TEST`. By default, (`conduitSubscriptions=true`), 15 messages will be sent to the consumer on broker A and the resulting 15 messages will be sent to the two consumers on broker B. The message load has not been equally spread across all three consumers because, by default, broker A views the two subscriptions on broker B as one. If you had set `conduitSubscriptions` to `false`, then each of the three consumers would have been given 10 messages.

#### Duplex network connectors

By default a network bridge forwards messages on demand in one direction over a single connection. When `duplex=true`, the same connection is used for a network bridge in the opposite directions, resulting in a bi-directional bridge. The network bridge configuration is propagated to the other broker so the duplex bridge is an exact replica or the original.

  
Given two brokers, broker A and broker B, a duplex bridge on A to B is the same as a default bridge on A to B and a default bridge on B to A.

  
Note, if you want to configure more than one duplex network bridge between two brokers, to increase throughput or to partition topics and queues, you must provide unique names for each:
```
<networkConnectors>
        <networkConnector name="SYSTEM1" duplex="true" uri="static:(tcp://10.x.x.x:61616)">
                <dynamicallyIncludedDestinations>
                        <topic physicalName="outgoing.System1" />
                </dynamicallyIncludedDestinations>
        </networkConnector>
        <networkConnector name="SYSTEM2" duplex="true" uri="static:(tcp://10.x.x.x:61616)">
                <dynamicallyIncludedDestinations>
                        <topic physicalName="outgoing.System2"/>
                </dynamicallyIncludedDestinations>
        </networkConnector>
  </networkConnectors>
```

#### Conduit subscriptions and consumer selectors

Conduit subscriptions ignore consumer selectors on the local broker and send all messages to the remote one. Selectors are then parsed on the remote brokers before messages are dispatched to consumers. This concept could create some problems with consuming on queues using selectors in a multi-broker network. Imagine a situation when you have a producing broker forwarding messages to two receiving brokers and each of these two brokers have a consumer with different selector. Since no selectors are evaluated on the producer broker side, you can end up with all messages going to only one of the brokers, so messages with certain property will not be consumed. If you need to support this use case, please turn off `conduitSubscription` feature.

#### Configuration Pitfalls

> Networks do not work as expected (they cannot dynamically respond to new consumers) if the `advisorySupport` broker property is disabled. A fully statically configured network is the only option if `advisorySupport` is disabled. Read more about it in the following section.

### Networks of brokers and advisories

Network of brokers relies heavily on advisory messages, as they are used under the hood to express interest in new consumers on the remote end. By default, when network connector starts it defines one consumer on the following topic `ActiveMQ.Advisory.Consumer.>` (let's ignore temporary destination for the moment). In this way, when consumer connects (or disconnects) to the remote broker, the local broker will get notified and will treat it as one more consumer it had to deal with.

This is all fine and well in small networks and environments whit small number of destinations and consumers. But as things starts to grow a default model (listen to everything, share everything) won't scale well. That's why there are many ways you can use to filter destinations that will be shared between brokers.

#### Authorization for advisories

If using the authorization plugin ensure that the user used for the network connections has permissions to the advisory destinations;

```
<broker> 
  .. 
    <plugins> 
      .. 
      <authorizationPlugin> 
        <map> 
          <authorizationMap> 
            <authorizationEntries> 
              ...
              <!-- Grant users that will be used to create a network of brokers permission for consumer and virtual destination consumer advisories -->
              <authorizationEntry topic="ActiveMQ.Advisory.Consumer.>" read="bridge-user,admin" write="bridge-user,admin" admin="bridge-user,admin"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.VirtualDestination.Consumer.>" read="bridge-user,admin" write="bridge-user" admin="bridge-user,admin"/> 
              ...
            </authorizationEntries> 
          </authorizationMap> 
        </map> 
      </authorizationPlugin> 
      .. 
    </plugins> 
  .. 
</broker>
```


#### Dynamic networks

Let's start with dynamically configured networks. This means that we only want to send messages to the remote broker when there's a consumer there. If we want to limit this behavior only on certain destinations we will use `dynamicallyIncludedDestinations`, like
```
<networkConnector uri="static:(tcp://host)">
  <dynamicallyIncludedDestinations>
    <queue physicalName="include.test.foo"/>
    <topic physicalName="include.test.bar"/>
  </dynamicallyIncludedDestinations>
</networkConnector>
```
In versions of ActiveMQ prior to 5.6, the broker would still use the same advisory filter and express interest in all consumers on the remote broker. The actual filtering will be done during message dispatch. This is suboptimal solution in huge networks as it creates a lot of "advisory" traffic and load on the brokers. Starting with version 5.6, the broker will automatically create an appropriate advisory filter and express interest only in dynamically included destinations. For our example it will be "`ActiveMQ.Advisory.Consumer.Queue.include.test.foo,ActiveMQ.Advisory.Consumer.Topic.include.test.bar`". This can dramatically improve behavior of the network in complex and high-load environments.

In older broker versions we can achieve the same thing with a slightly more complicated configuration. The actual advisory filter that controls in which consumers we are interested is defined with the `destinationFilter` connector property. Its default value is ">", which is concatenated to the `"ActiveMQ.Advisory.Consumer."` prefix. So to achieve the same thing, we would need to do the following:
```
<networkConnector uri="static:(tcp://host)" destinationFilter="Queue.include.test.foo,ActiveMQ.Advisory.Consumer.Topic.include.test.bar">
  <dynamicallyIncludedDestinations>
    <queue physicalName="include.test.foo"/>
    <topic physicalName="include.test.bar"/>
  </dynamicallyIncludedDestinations>
</networkConnector>
```
Note that first destination does not have the prefix because it's already implied. It's a bit more complicated to set and maintain, but it will work. And if you're using 5.6 or newer version of the broker just including desired destinations with `dynamicallyIncludedDestinations` should suffice.

This also explains why dynamic networks do not work if you turn off advisory support on the brokers. The brokers in this case cannot dynamically respond to new consumers.

#### Pure static networks

If you wish to completely protect the broker from any influence of consumers on the remote broker, or if you wish to use the brokers as a simple proxy and forward all messages to the remote side no matter if there are consumers there or not, static networks are something you should consider.
```
<networkConnector uri="static:(tcp://host)" staticBridge="true">
        <staticallyIncludedDestinations>
      		<queue physicalName="always.include.queue"/>
        </staticallyIncludedDestinations>
</networkConnector>
```
The `staticBridge` parameter is available since version 5.6 and it means that the local broker will not subscribe to any advisory topics on the remote broker, meaning it is not interested in whether there are any consumers there. Additionally, you need to add a list of destinations to `staticallyIncludedDestinations`. This will have the same effect as having an additional consumer on the destinations so messages will be forwarded to the remote broker as well. As there is no `staticBridge` parameter in the earlier versions of ActiveMQ, you can trick the broker by setting `destinationFilter` to listen to an unused advisory topic, like
```
<networkConnector uri="static:(tcp://host)" destinationFilter="NO_DESTINATION">
        <staticallyIncludedDestinations>
      		<queue physicalName="always.include.queue"/>
        </staticallyIncludedDestinations>
</networkConnector>
```
If configured like this, broker will try to listen for new consumers on `ActiveMQ.Advisory.Consumer.NO_DESTINATION`, which will never have messages so it will be protected from information on remote broker consumers.

### Dynamic networks and Virtual Destinations (New for 5.13.0)

As described above, a network of brokers can be configured to only send messages to a remote broker when there's a consumer on an included destination.  However, let's consider some cases of how dynamic flow occurs when [Virtual Destinations](virtual-destinations) are in use.

#### Virtual Destination consumers and Composite Destinations

Here is an example of two brokers networked together.  The Local Broker contains the network connector configured with a `dynamicallyIncludedDestination` and the Remote Broker is configured with a CompositeTopic:

**Local Broker**
```
<networkConnector uri="static:(tcp://host)">
	<dynamicallyIncludedDestinations>
    	<topic physicalName="include.bar"/>
	</dynamicallyIncludedDestinations>
</networkConnector>
```

**Remote Broker**
```
<compositeTopic name="include.bar" forwardOnly="false">
    <forwardTo>
        <queue physicalName="include.bar.forward" />
    </forwardTo>
</compositeTopic >
```
In this example, let's consider a single consumer on the Remote Broker on the queue `include.bar.forward`.  If a message is sent directly to the Remote Broker on the topic `include.bar`, it will be forwarded to the queue `include.bar.forward` and the consumer will receive it.  However, if a message is published to the same topic on the Local Broker, this message will not be forwarded to the Remote Broker.

The message is not forwarded because a consumer on the `queue include.bar.forward` would not be detected as part of the `dynamicallyIncludedDestinations` list in the Local Broker.  Messages would not be forwarded to the Remote Broker unless there was a consumer on the original topic as well (in this case `include.bar`).  This can be fixed by configuring the Local Broker to listen for Virtual Destination Subscriptions.  

First, we need to configure the Remote Broker to send advisory messages when consumers subscribe to a destination that matches a Virtual Destination.  In this case, internally a match is determined through the use of a Destination Filter that determines whether one destination forwards to another destination.  To enable this, set the property `useVirtualDestSubs` on the Remote Broker to `true`:

**Remote Broker**
```
<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://activemq.org/config/1.0">

  <broker name="remoteBroker" useVirtualDestSubs="true">  
     .....
  </broker>

</beans>
```
  
Next, the network connector on the Local Broker needs to be configured to listen for the new advisory messages by setting the `useVirtualDestSubs` property to `true`:

**Local Broker**
```
<networkConnector uri="static:(tcp://host)" useVirtualDestSubs="true">
  <dynamicallyIncludedDestinations>
    <topic physicalName="include.bar"/>
  </dynamicallyIncludedDestinations>
</networkConnector>
```
Now, if a consumer comes subscribes to the queue `include.bar.forward` on the Remote Broker, the Local Broker will forward messages that are sent to the topic `include.bar.`

#### Virtual Destination Consumers on Destination Creation

Now let's consider the use case above where there is the same composite topic but no consumers on the queue.  
 
**Remote Broker**
```
<compositeTopic name="include.bar" forwardOnly="false">
    <forwardTo>
        <queue physicalName="include.bar.forward" />
    </forwardTo>
</compositeTopic >
```
There is a Composite Topic configured on the Remote Broker, and the Local Broker is networked to it.  

Even though we have enabled `useVirtualDestSubs`, messages will not be forwarded to the Remote Broker unless a consumer subscribes to the forwarded queue.  Without a consumer, messages would be dropped as they are sent to a topic on the Local Broker (`include.bar`).  In this situation it is desirable to have messages forwarded based on the existence of a virtual destination that forwards to:

*   one or more queues; or
*   topics that have durable subscriptions.

Both of these conditions are considered as emitting demand for messages to the Local Broker, despite there being no active consumers on those destinations.

**Remote Broker**
```
<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://activemq.org/config/1.0">

  <broker name="remoteBroker" useVirtualDestSubs="true" useVirtualDestSubsOnCreation="true">  
     .....
  </broker>

</beans>
```
With this configuration, when the queue `include.bar.forward` is created, a Virtual Destination consumer advisory will be sent to the Local Broker so that it knows to forward messages based on the existence of the Composite Topic that forwards to a queue.

#### Composite Destination consumers and Virtual Topics 

The above examples show how to configure a Composite Destination but a Virtual Topic will also work.  In the example below, a consumer on a queue for a Virtual Topic on the Remote Broker will now cause demand and messages will be sent across a network from the Local Broker.

**Local Broker**
```
<networkConnector uri="static:(tcp://host)">
  <dynamicallyIncludedDestinations>
    <topic physicalName="VirtualTopic.>"/>
  </dynamicallyIncludedDestinations>
</networkConnector>
```

**Remote Broker**
```
<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://activemq.org/config/1.0">

  <broker name="remoteBroker" useVirtualDestSubs="true" >  
     .....
  </broker>

</beans>
```

### Example Configuration using NetworkConnector properties

This part of an example configuration for a Broker
```
<networkConnectors>
      <networkConnector uri="static:(tcp://localhost:61617)"
         name="bridge"
         conduitSubscriptions="true"
         decreaseNetworkConsumerPriority="false">
      	<dynamicallyIncludedDestinations>
      		<queue physicalName="include.test.foo"/>
      		<topic physicalName="include.test.bar"/>
      	</dynamicallyIncludedDestinations>
      	<excludedDestinations>
      		<queue physicalName="exclude.test.foo"/>
      		<topic physicalName="exclude.test.bar"/>
      	</excludedDestinations>
        <staticallyIncludedDestinations>
      		<queue physicalName="always.include.queue"/>
      		<topic physicalName="always.include.topic"/>
      	</staticallyIncludedDestinations>
      </networkConnector>
    </networkConnectors>
```
Note that at the moment `excludedDestinations` property doesn't affect `staticallyIncludedDestinations`.

It is possible to have more than one network connector between two brokers. Each network connector uses one underlying transport connection, so you may wish to do this to increase throughput, or have a more flexible configuration.
  
For example, if using distributed queues, you may wish to have equivalent weighting to queue receivers across the network, but only when the receivers are active - e.g.
```
<networkConnectors>
      <networkConnector uri="static:(tcp://localhost:61617)"
         name="queues_only"
         conduitSubscriptions="false"
         decreaseNetworkConsumerPriority="false">
      	<excludedDestinations>
      		<topic physicalName=">"/>
      	</excludedDestinations>
      </networkConnector>
    </networkConnectors>
```

**N.B.** You can only use [wildcards](wildcards) in the `excludedDestinations` and `dynamicallyIncludedDestinations` properties.  
**N.B.** **Do not** change the name of the bridge or the name of the Broker if you are using durable topic subscribers across the network. Internally ActiveMQ uses the network name and broker name to build a unique but repeatable durable subscriber name for the network.

### Stuck Messages

There might have multiples cause for Stuck Message, sections below aim to give advice setting that may help to fix this issue.

#### Disable replayWhenNoConsumers (version 5.6)
There is a destination policy that allows this behavior for queues by configuring a `conditionalNetworkBridgeFilterFactory` with `replayWhenNoConsumers=true`. The `conditionalNetworkBridgeFilterFactory` provides an optional `replayDelay` based on the broker-in time.
```
    <destinationPolicy>
      <policyMap>
        <policyEntries>
          <policyEntry queue="TEST.>" enableAudit="false">
            <networkBridgeFilterFactory>
              <conditionalNetworkBridgeFilterFactory replayWhenNoConsumers="true"/>
            </networkBridgeFilterFactory>
          </policyEntry>
        </policyEntries>
      </policyMap>
    </destinationPolicy>
```

**N.B.:** When using `replayWhenNoConsumers=true` for versions < 5.9, it is necessary to also disable the cursors duplicate detection using `enableAudit=false` as the cursor could mark the replayed messages as duplicates (depending on the time window between playing and replaying these messages over the network bridge). The problem is fully explained in this [blog post](http://tmielke.blogspot.de/2012/03/i-have-messages-on-queue-but-they-dont.html).

#### Activate decreaseNetworkConsumerPriority

Set decreaseNetworkConsumerPriority="true" in the networkConnector to limit the number of advisory and prefer local consumers. (more information on [AMQ-7316](https://issues.apache.org/jira/browse/AMQ-7316 )

    <networkConnectors>
      <networkConnector uri="static:(tcp://mybroker:61616)" decreaseNetworkConsumerPriority="true" />
    </networkConnectors>


#### Increase prefetchsize if you're using Message Group and Consumer Pool

If message group is enabled, one group might block the replication of a queue (if the size of message in this group is higher than the configured prefetchsize), providing a prefetchSize sufficient to ensure that all comsumers threads can consume their attributed group.

#### Increase TTL

TTL setting (networkTTL, messageTTL and consumerTTL) for the Network of Brokers should be high enough to allow messages exchange between the brokers several times in case the consumer
fails back and forth several times. 


### Throttling a network consumer

The `conditionalNetworkBridgeFilterFactory` factory allows a rate limit to be specified for a destination, such that network consumer can be throttled. Prefetch for a network consumer is largely negated by the fact that a network consumer relays a message typically acks very quickly so even with a low prefetch and decreased priority a network consumer can starve a modestly quick local consumer. Throttling provides a remedy for this.
