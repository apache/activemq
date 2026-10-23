---
title: "Message Groups"
slug: /message-groups
---


Message Groups
--------------

Message Groups are an enhancement to the [Exclusive Consumer](exclusive-consumer) feature. They provide:

*   Guaranteed ordering of the processing of related messages across a single queue.
*   Load balancing of the processing of messages across multiple consumers.
*   High availability / auto-failover to other consumers if a JVM goes down.

So logically Message Groups are like a parallel [Exclusive Consumer](exclusive-consumer). Rather than all messages going to a single consumer, the standard JMS header `JMSXGroupID` is used to define which _message group_ the message belongs to. The Message Group feature then ensures that all messages for the _same_ message group will be sent to the _same_ JMS consumer - whilst that consumer stays alive. As soon as the consumer dies another will be chosen.

Another way of explaining Message Groups is that it provides sticky load balancing of messages across consumers; where the `JMSXGroupID` is kinda like a HTTP session ID or cookie value and the message broker is acting like a HTTP load balancer.

### Example Use Case

Lets say we are doing some kind of order matching system where people are buying and selling things (stocks, shares, placing online bets, whatever). You want to have consumers who match bids and offers for different items (stocks / bets) so they want to keep in RAM for performance a sub-set of the data set. Therefore set the `JMSXGroupID` to be `MSFT`, `IBM`, `SUNW` and so forth to use the stock symbol to define the message group. (It can be any string whatsoever; maybe combining trading book, trading exchange, date and so forth - the more specific the group ID, the more concurrent you can run). Assume we are buying and selling `MSFT`, `IBM`, `SUNW` shares; the Message Groups feature guarantees that all the `MSFT` messages will be processed in order by the same consumer; ditto for `IBM` and `SUNW`.

### How Message Groups Work

When a message is being dispatched to a consumer, the `JMSXGroupID` is checked. If one is present then the broker checks to see if a consumer owns that message group. Since there could be a large number of message groups hash buckets are used rather than the actual `JMSXGroupID` string.

If no consumer is associated with a message group a consumer is chosen. Said JMS `MessageConsumer` will receive all further messages with the same `JMSXGroupID` value until:

*   the consumer closes (or the client which created the consumer dies etc).
*   someone closes the message group by sending a message with a negative value for `JMSXGroupSeq` (see below for more details).

**Note**: as with message selector matching, grouping based on `JMSXGroupID` occurs before dispatch on messages in memory. With the default `maxPageSize` option, large backlogs of messages destined for one group can block receipt of messages to other groups if they don't all fit in memory. You can change the default `maxPageSize` setting for destinations as follows:
```
<destinationPolicy>
    <policyMap>
       <policyEntries>
           <policyEntry queue=">" maxPageSize="1000"/>
       </policyEntries>
    </policyMap>
</destinationPolicy>
```

### Using Message Groups

You just need to change your JMS producers to fill in the `JMSXGroupID` message header with some `String` value of your choice.

Example:
```
Mesasge message = session.createTextMessage("<foo>hey</foo>");
message.setStringProperty("JMSXGroupID", "IBM\_NASDAQ\_20/4/05");
...
producer.send(message);
```

### Closing a Message Group

You generally don't need to close a message group; just keep using it. However if you really do want to close a group you can add a negative sequence number.

Example:
```
Mesasge message = session.createTextMessage("<foo>hey</foo>");
message.setStringProperty("JMSXGroupID", "IBM\_NASDAQ\_20/4/05");
message.setIntProperty("JMSXGroupSeq", -1);
...
producer.send(message);
```
This then _closes_ the message group so if another message is sent in the future with the same message group ID it will be reassigned to a new consumer.

### Implications

Message Groups mean you get the power of **grid** processing of messages across a cluster of consumers with reliability, auto-failover, load balancing but you can also order the processing of messages too. So its the best of both worlds. However using the above example, what Message Groups actually do is to partition your work load across consumers using a user definable partition strategy - the `JMSXGroupID` value.

The neat thing about this is that you can do neat things like use lots of RAM caching; keep the order for `MSFT` in RAM in the `MSFT` consumer; keep the `IBM` orders in RAM in the `IBM` consumer - since the message broker is partitioning for you, you do not have to rely on a distributed cache with inter-cache synchronization and locking to take advantage of caching.

The great thing is - to the application developer, it looks like a simple 1 consumer world where you process messages and do your job; leaving the broker to do all the hard stuff for you

*   partitioning the traffic
*   load balancing of message groups across consumers
*   auto-failover of groups to different consumers as consumers come and go

In summary; if ordering or per-message caching and synchronization are in any way important to you then we highly recommend you use message groups to partition your traffic.

### Getting Notified of Ownership Changes of Message Groups

ActiveMQ support a boolean header called `JMSXGroupFirstForConsumer`. This header is set on the first message sent to a consumer for a particular message group.

If the JMS connection is using `failover:` and a temporary network error occurs so that the connection disconnects from the broker and reconnects some time later, a new consumer instance will be created under the covers of the JMS client leading to the possibility of another message with this header being set for the same message group.

Example:
```
String groupId = message.getStringProperty("JMSXGroupId");

if (message.getBooleanProperty("JMSXGroupFirstForConsumer")) {
   // flush cache for groupId
}
```
To flush caches to ensure consistent state when faced with network errors.

### Adding New Consumers

If you have existing messages in the broker and add consumers at a later stage, it is a good idea to delay message dispatch start until all consumers are present (or at least to give enough time for them to subscribe). If you don't do that the first consumer will probably acquire all message groups and all messages will be dispatched to it. You can achieve this by using `consumersBeforeDispatchStarts` and `timeBeforeDispatchStarts` [destination policies](per-destination-policies).

When both **consumersBeforeDispatchStarts** and **timeBeforeDispatchStarts** are set to a value greater than zero, the dispatching will start as soon as the required number of consumers are present or the timeBeforeDispatchStarts timeout expires. If only consumersBeforeDispatchStarts is set then the timeout for consumers to connect is 1 second. If all consumers disconnect then message dispatch delay will be applied again at the next consumer connection.

Here's the example of the destination policy that delays dispatch for `200ms`:
```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue=">" timeBeforeDispatchStarts="200"/>
    </policyEntries>
  </policyMap>
</destinationPolicy>
```
The following code snippet shows how to wait for two consumers (or two seconds) before dispatch starts:
```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue=">" consumersBeforeDispatchStarts="2" timeBeforeDispatchStarts="2000"/>
    </policyEntries>
  </policyMap>
</destinationPolicy>
```
As [the appropriate test case](https://github.com/apache/activemq/blob/master/activemq-unit-tests/src/test/java/org/apache/activemq/usecases/MessageGroupDelayedTest.java) shows, adding a small time pause before dispatching or setting a minimum consumer number, ensures equal message group distribution.

If you need to rebalance the message groups manually for any reason you can do so by executing the `removeAllMessageGroups` operation on the JMX MBean of the corresponding queue.

### Competing demands of memory consumption, load balancing, complexity, etc.

The default behavior called `CachedMessageGroupMap` is limited to 1024 message groups in an LRU cache may not match you expectation w.r.t message order. `CachedMessageGroupMap` has bounded memory use, but only keeps track of up to 1024 (or the maximum configured size) groups, then loses track of any groups older than the newest 1024. In this way, if there are more groups than the maximum, **ordering will be lost for the oldest groups**.

Typically users would close groups such that the in memory set can be retained below the configured limits. Some usefull discussion at [AMQ-6851](https://issues.apache.org/jira/browse/AMQ-6851)

In order to prevent this limitation you can use `MessageGroupHashBucket` or `SimpleMessageGroupMap`. Their are working by associating each group with a consumer.

`SimpleMessageGroupMap` keeps track of every group but suffers from unbounded memory use.

The following code snippet shows how to enable it :

```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue=">">
          <messageGroupMapFactory>
            <simpleMessageGroupMapFactory/>
          </messageGroupMapFactory>
      </policyEntry>
    </policyEntries>
  </policyMap>
</destinationPolicy>

```

`MessageGroupHashBucked` keeps track of every group and has bounded memory use. The following code snippet shows how to enable it :

```
<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry queue=">">
          <messageGroupMapFactory>
            <messageGroupHashBucked cachedSize=1024 />
          </messageGroupMapFactory>
      </policyEntry>
    </policyEntries>
  </policyMap>
</destinationPolicy>
```

### See

*   [How do Message Groups compare to Selectors](how-do-message-groups-compare-to-selectors)
