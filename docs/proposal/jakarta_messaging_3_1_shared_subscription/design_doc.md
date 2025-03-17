# JMS 2.0 / Jakarta Messaging 3.1: Shared Subscriptions

## Problem statement

JMS 2.0 / Jakarta Messaging 3.1 introduced a new set of  API for creating shared topic subscriptions that is not yet supported by ActiveMQ Classic. Even though the functionality can be achieved using virtual topics in ActiveMQ Classic, to be Jakarta Messaging 3.1 compliant, we will need to support that API. This document proposes several designs and evaluates their tradeoffs.

## Background

A JMS durable subscriber MessageConsumer is created with a unique JMS clientID and durable subscriber name. In JMS 1.1 only one JMS connection can be active at any point in time for one JMS clientID, and only one consumer can be active for a clientID and subscriber name. i.e., only one thread can be actively consuming from a given logical topic subscriber. This limits load balancing of messages or fast failover of the subscriber if that one process running that one consumer thread dies.

Virtual topics were introduced to mitigate these issues. The idea behind virtual topics is that producers send to a topic in the usual JMS way. Consumers can continue to use the Topic semantics in the JMS specification. However if the topic is virtual, consumers can consume from a physical queue for a logical topic subscription, allowing many consumers to be running on many machines & threads to balance the load.

JMS 2.0 and Jakarta 3.1 support shared subscriptions which enable durable and non-durable topic subscriptions that can be shared between any number of consumers. This allows consumers to share the work of consuming messages from a Topic which improves performance and reliability. This is a simpler solution than Virtual Topics as consumers can directly subscribe to the topic and not to underlying queues

## High level design approach

### Option 1: Reuse as much of the virtual topics implementation as possible.

This approach will implement shared subscription as a wrapper of virtual topics.

For shared subscriptions, extra logic would be needed to manage the consumer subscription to the queue as the queue internals should not be exposed to the users. An underlying queue could be created for each shared subscription on invocation of *`createSharedDurableConnectionConsumer`*. A mapping would be needed to identify which shared subscription maps to an underlying queue on the virtual topic(perhaps in *`QueueRegion`*). For example when the *`addConsumer`* method of *`AbstractRegion`* is invoked, a new queue would need to be created and the mapping from the subscription key to this queue created. When new shared subscriptions for the consumer are created then the mapping would ensure the subscription is created on the underlying queue for that subscription key.  
A *`VirtualTopicInterceptor`* overrides *`send`* to ensure messages get delivered to all destinations matching a wildcard. For shared subscriptions an interceptor could be created which sends messages to only one destination in a group of destinations matching a wildcard. This could be achieved in a round robin fashion.

**Pros:**

- Reuses logic from existing virtual topics and stays in line with existing solutions.

**Cons:**

- This may not be solving the problem at the right level. Shared subscriptions are applied at a subscription level, not a destination level.
- Extra logic is needed to correlate a shared subscription to a queue.
- The underlying queues would show in the ActiveMQ admin console. This may be confusing for users. It may also require protection to ensure these queues are not deleted or purged.

### Option 2 **\[Recommended\]**: Handling shared subscription logic separately

There already exists logic for effectively managing subscriptions in the *`DurableTopicSubscription`* and *`TopicSubscription`* classes. The main difference between what exists here and what is needed for shared subscriptions is the ability for the class to act as a composite subscription. The *`DurableTopicSubscription`* and *`TopicSubscription`* classes have already been optimized to use cursors to effectively page in messages for delivery. Messages could be dispatched from the *`SharedDurableTopicSubscription`* cursor to one of the cursors of the underlying subscriptions via round robin. A map from subscription key to shared subscription would be needed in the *`TopicRegion.`* This is for mappings and also to perform validation checks on the shared subscriptions. This could be managed in a similar way to the existing *`ConcurrentMap<SubscriptionKey, DurableTopicSubscription> durableSubscriptions`* map.

![option_2](img/img_1.png)

**Pros:**

- Cursors reduce the impact on RAM for in transit messages.
- Messages can be passed from producer to consumers directly if they are fast.

**Cons:**

- Implements a similar offering as virtual topics in a very different way. 

## Detailed Design

Option 2 is recommended and in this section we will focus on implementation design detail of option 2, handling shared subscription logic separately. The detailed design is divided into 7 categories.

### 1\. Shared durable subscription creation

When *`addConsumer`* is called on *`TopicRegion`* it uses the *`ConsumerInfo`* to determine if the subscription is durable. *`ConsumerInfo`* will need to be updated to include a new boolean *`shared`* property for determining if a consumer is also shared. *`TopicRegion`* maintains a *`ConcurrentMap<SubscriptionKey, DurableTopicSubscription>`* called *`DurableSubscriptions`* which it checks for existing subscriptions and throws a *`JMSException`* if the subscription key is already in use. It will also have a *`ConcurrentMap<SubscriptionKey, SharedTopicSubscription>`* called *`SharedSubscriptions`* which will be used to identify if a subscription is a shared subscription. This map can be updated when *`addConsumer`* invokes *`createSubscription`* on the *`TopicRegion`*. *`createSubscription`* will need to be updated with logic to create a new *`SharedDurableTopicSubscription`* or *`SharedNonDurableTopicSubscription`* . A new interface *`SharedTopicSubscription`* will extend the *`Subscription`* interface. It will be used to define methods for managing the shared consumers on a *`SharedSubscription`*. For example

```java
public interface SharedTopicSubscription extends Subscription {
    public Set<Subscription> getSharedConsumers();
    public void addSharedConsumer(Subscription subscription);
    public void removeSharedConsumer(Subscription subscription);
}
```
*`SharedDurableTopicSubscription`* and *`SharedNonDurableTopicSubscription`* will both implement the *`SharedTopicSubscription`* interface.

![creation_of_shared_subscription](img/img_2.png)

The *`DurableSubscriptions`* map on *`TopicRegion`* can be used to track if a shared subscription is also durable. *`addConsumer`* will also invoke its superclass *`AbstractRegion`* and will need to put the new *`SharedTopicSubscription`* to the *`Map<ConsumerId, Subscription>`* map called *`subscriptions`*. This method will also call *`addSubscription`* on all relevant Topics to add the new *`SharedTopicSubscription`*.  
The *`addConsumer`* method of *`TopicRegion`* can have additional checks for shared subscriptions. If an attempt is made to create a consumer which matches a subscription key in the *`SharedSubscriptions`* map then the request will be validated to ensure the same topic and message selector is used. Since shared subscriptions will also be added to the *`DurableSubscriptions`* map if durable, this map can be used to validate that a shared durable subscription and an unshared durable subscription may not have the same name and client identifier.

![flow_chart](img/img_3.png)

Link stealing allows a new client connection with the same client ID as an existing client to steal the connection. The existing client’s session will be closed and its network connection will be terminated. Link stealing will not be supported initially for shared consumers.

The *`addSubscriptionsForDestination`* method of *`TopicRegion`* restores all durable subscriptions on a Topic when a connection is first added to that destination. A *`SharedDurableTopicSubscription`* should be restored as a *`DurableTopicSubscription`* by default. The *`addSubscriptionsForDestination`* method has logic to determine this based on the restored *`SubscriptionInfo`*. If a shared durable consumer is added using the same subscription key, then the *`DurableTopicSubscription`* can become a *`SharedDurableTopicSubscription`* as long as no active consumers exist.

Cursors can be overridden in the activemq.xml configuration. If a user overrides the cursor for a *`SharedTopicSubscription`* then the subscription should still behave as expected as all cursors implement the *`PendingMessageCursor`* interface. For example if *`<pendingSubscriberPolicy> <fileCursor/> </pendingSubscriberPolicy>`* is used then the *`SharedTopicSubscription`*  should use a *`fileCursor`* instead of a *`vmCursor`*.

It may be possible for a rogue client to create an excessive number of concurrent consumers on a shared subscription. Hence it’s reasonable to expose a property that users can set which limits the number of consumers on a shared subscription. The default for this property could be \-1 denoting no limit.


### 2\. Message delivery to shared durable subscriptions

A new class *`SharedDurableTopicSubscription`* is created which extends *`DurableTopicSubscription`* . This class can reuse a lot of the logic from a *`DurableTopicSubscription`* for persistent dispatch of messages. It holds a set of *`SharedSubscription`* which are responsible for distributing messages to the shared consumers.  
*`SharedSubscription`* is a new class which extends *`PrefetchSubscription`* and implements abstract methods like *`acknowledge`*.  
The *`SharedDurableTopicSubscription`* has a *`PendingMessageCursor`* which is a  
*`StoreDurableSubscriberCursor`* containing messages for dispatch. Each of the *`SharedSubscriptions`* have their own *`VMPendingMessageCursor`* which contains messages to be dispatched to each consumer. The *`add`* method of *`SharedDurableTopicSubscription`* will add a message to its *`StoreDurableSubscriberCursor`* . It will then do a round robin to decide which subscription gets the message added to their *`VMPendingMessageCursor`*.

![](img/img_4.png)

The *`SharedSubscription`* will then invoke *`DispatchPending`* to dispatch messages to the relevant consumer via the appropriate transport connection. When messages are dispatched they are removed from the *`pending` `MessageCursor`* and added to a list of *`MessageReference`* called *`dispatched`*. The *`SharedDurableTopicSubscription`* will also move the dispatched message from it’s *`pending` `MessageCursor`* to it’s *`dispatched`* list. The *`dispatched`* list reflects messages that are dispatched to consumers and awaiting acknowledgement. Should a consumer fail to process the dispatched message, the message can move from *`dispatched`* back onto the *`pending` `MessageCursor`*.

![](img/img_5.png)

When *`acknowledge`* is invoked on the *`AbstractRegion`*(which *`TopicRegion`* extends) it gets the relevant subscription from the *`ConsumerBrokerExchange`*. In this case it would be a *`SharedSubscription`* to which the message was sent. The *`acknowledge(final ConnectionContext context,final MessageAck ack)`* method from *`PrefetchSubscription`*(which *`SharedSubscription`* extends) is called to remove the appropriate messages from the *`dispatched`* list. It would then invoke the overridden *`acknowledge`* method on *`SharedSubscription`* which could be used to call back to the *`acknowledge`* method on it’s parent *`SharedDurableTopicSubscription`* .

![](img/img_6.png)

When a consumer invokes *`pullMessage`* on a *`SharedSubscription`* then the *`prefetchExtension`* of the subscription is set to the pull quantity. *`dispatchPending`* is then triggered on the parent *`SharedDurableTopicSubscription`* doing a round robin on subscriptions that have space in their prefetch. This means that *`SharedSubscription`s* on other consumers may also receive the messages in pending. This is to maintain balanced message distribution across consumers.

### 3\. Message delivery to shared non-durable subscriptions

A new class *`SharedNonDurableTopicSubscription`* is created which extends *`TopicSubscription`* . This class can reuse a lot of the logic from a *`TopicSubscription`* for dispatch of messages in memory. Similar to *`SharedDurableTopicSubscription`*`,` it holds a set of *`SharedSubscription`* which are responsible for distributing messages to the shared consumers.  
The *`SharedNonDurableTopicSubscription`* has a *`PendingMessageCursor`* which is a  
*`VMPendingMessageCursor`* containing messages for dispatch. Each of the *`SharedSubscriptions`* have their own *`VMPendingMessageCursor`* which contains messages to be dispatched to each consumer. The *`add`* method of *`SharedDurableTopicSubscription`* will add a message to its *`VMPendingMessageCursor`* . It will then do a round robin to decide which subscription gets the message added to their *`VMPendingMessageCursor`*.

![](img/img_7.png)

The *`SharedSubscription`* will then invoke *`DispatchPending`* to dispatch messages to the relevant consumer via the appropriate transport connection. When messages are dispatched they are removed from the *`pending`* *`MessageCursor`* and added to a list of *`MessageReference`* called *`dispatched`*. The *`SharedNonDurableTopicSubscription`* will also move the dispatched message from its *`matched`* *`MessageCursor`* to its *`dispatched` `DispatchNode`* list.

![](img/img_8.png)

Similar to *`SharedDurableTopicSubscription`* the *`acknowledge(final ConnectionContext context,final MessageAck ack)`* method from *`PrefetchSubscription`*(which *`SharedSubscription`* extends) is called to remove the appropriate messages from the *`dispatched`* list. It would then invoke the overridden *`acknowledge`* method on *`SharedSubscription`* which could be used to call back to the *`acknowledge`* method on its parent *`SharedNonDurableTopicSubscription`* .

![](img/img_9.png)

When a consumer invokes *`pullMessage`* on a *`SharedSubscription`* then the *`prefetchExtension`* of the subscription is set to the pull quantity. *`dispatchMatched`* is then triggered on the parent *`SharedNonDurableTopicSubscription`* doing a round robin on subscriptions that have space in their prefetch. This means that *`SharedSubscription`s* on other consumers may also receive the messages in pending. This is to maintain balanced message distribution across consumers.

![](img/img_10.png)

### 4\. Shared durable subscription deletion

When a *`TransportConnection`* stops, it invokes the *`removeConsumer`* method of *`TopicRegion`*. If the consumer matches belong to a shared subscription then it should be removed from the relevant *SharedDurableTopicSubscription*.

![](img/img_11.png)

A durable subscription will continue to accumulate messages until it is deleted using a *`RemoveSubscriptionInfo`* command. This will invoke *`removeSubscription`* on the *`TopicRegion`* which removes durable subscriptions from the *`durableSubscriptions`* map. If a durable subscription is still active then this will throw a *`JMSException`*. For a *`SharedDurableTopicSubscription`* it’s *`isActive`* method will check all contained *`SharedSubscription`s* to see if any of them are active. The *`SharedSubscriptions`* map should also be updated to remove the *`SharedTopicSubscription`*. The *`TopicRegion`* also calls *`deleteSubscription`* for every *`Topic`* in it’s *`destinations`* which deletes subscriptions from the *`TopicStore`*. Finally, *`removeConsumer`* is invoked on the superclass *`Abs`t`ractRegion`* which removes the consumer from the *`subscriptions`* map and calls *`removeSubscription`* on all destinations.

![](img/img_12.png)

### 5\. Shared non-durable subscription deletion

A shared non-durable subscription will be deleted when the last consumer on the subscription is closed. When a *`TransportConnection`* stops, it invokes the *`removeConsumer`* method of *`TopicRegion`*. The *`SharedSubscriptions`* map should also be updated to remove the *`SharedTopicSubscription`* if it contains no more consumers. Otherwise it should just remove its consumer matching the *`consumerId`* from *`ConsumerInfo`*. *`removeConsumer`* of the superclass *`AbstractRegion`* is called which will remove the *`SharedNonDurableTopicSubscription`* from the *`subscriptions`* map and calls *`removeSubscription`* on all relevant destinations.

![](img/img_13.png)

### 6\. Client side methods changes

New *`createSharedConsumer`* and *`createSharedDurableConsumer`* methods have been added to *`Session`*, *`TopicSession`* and *`JMSContext`*.

*`Session`* has 6 implementations:

* *`ActiveMQSession`* ← Will need to be updated with logic to create shared consumers.
* *`ActiveMQQueueSession`* ← Should continue to throw exceptions as queues don’t support shared consumers.
* *`ActiveMQTopicSession`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too.
* *`PooledSession`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too.
* *`InboundSessionProxy`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too.
* *`ManagedSessionProxy`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too.

*`TopicSession`* extends *`Session`* and has 2 implementations not covered above:

* *`ActiveMQTopicSession`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too. Note this API exists for compatibility with the earliest JMS spec JMS 1.0.
* *`ActiveMQXASession`* ← Extends ActiveMQSession so will take advantage of the logic added there for shared consumers.

*`JMSContext`* has 1 implementation not covered above:

* *`ActiveMQContext`* ← Will need to be updated with logic to create shared consumers.

There is little to no information in the spec about support for shared consumers in *`ConnectionConsumer`* but it is still a Jakarta Messaging 3.1 interface. New *`createSharedConnectionConsumer`* and *`createSharedDurableConnectionConsumer`* methods have been added to the jakarta *`Connection`* interface.

*`Connection`* has 4 implementations:

* *`ActiveMQConnection`* ← Will need to be updated with logic to create shared consumers.
* *`ActiveMQXAConnection`* ← Extends *`ActiveMQConnection`* so will take advantage of the logic added there for shared consumers.
* *`PooledConnection`* ← Will need to be updated with logic to create shared consumers.
* *`InboundConnectionProxy`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too.
* *`ManagedConnectionProxy`* ← Currently doesn’t support creation of durable consumers. If durable consumers are not supported then it is unlikely that shared consumers would be too.

### 7\. Openwire changes

*`ConsumerInfo`* will need to be updated to include a new boolean *`shared`* property for determining if a consumer is shared. The current *`ConsumerInfo`* also determines durability based on *`return subscriptionName != null;`*. This will no longer hold true as the *`subscriptionName`* will now be set for non durable shared subscriptions. This means a new boolean *`durable`* property will also need to be added to *`ConsumerInfo`*.   
It may be best to introduce a new v13 of openwire for Jakarta 3.1. This means users will need to use the latest version of the openwire to have the version of *`ConsumerInfoMarshaller`* which creates shared subscriptions. Older openwire marshellers will not be updated with any of the changes to populate the new fields on *`ConsumerInfo`* for shared subscriptions.   
The *`isDurable`* method of *`ConsumerInfo`* will need to be updated to work with all versions of openwire. It will need the following evaluation:  
*`return (!shared && subscriptionName != null) || (shared && durable);`*.   
*`shared`* will default to false for openwire versions older than v13 and so *`isDurable`* will evaluate in the same way as previously, based on *`subscriptionName`*. For openwire v13 the *`shared`* and *`durable`* fields will be populated and so can be used to determine durability for shared subscriptions.

## Appendix

Links to the official spec:

* [https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1\#shared-non-durable-subscriptions](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-non-durable-subscriptions)
* [https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1\#shared-durable-subscriptions](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-durable-subscriptions)
* [https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1\#jakarta-messaging-application-server-facilities](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-application-server-facilities)
* [https://www.oracle.com/technical-resources/articles/java/jms2messaging.html](https://www.oracle.com/technical-resources/articles/java/jms2messaging.html)