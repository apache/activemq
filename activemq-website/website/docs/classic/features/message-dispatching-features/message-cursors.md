---
title: "Message Cursors"
slug: /message-cursors
---


Message Cursors
---------------

A common problem in previous versions of ActiveMQ was [running out of RAM buffer](my-producer-blocks) when using non-persistent messaging.

Beginning with ActiveMQ 5.0.0, there is a new memory model that allows messages to be paged in from storage when space is available (using Store cursors for persistent messages).

Releases prior to 5.0 kept references in memory for all the messages that could be dispatched to an active Durable Topic Consumer or a Queue. While a reference itself is not large, it does impose a limit on the maximum number of messages that can be pending delivery.

A typical approach for messaging systems dispatching persistent messages is to pull them in batches from long term storage when a client is ready to consume them, using a cursor to maintain the next to dispatch position. This is a robust and very scalable approach, but not the most performant for cases when the consumer(s) can keep up with the producer(s) of messages.

ActiveMQ 5.0 takes a hybrid approach, allowing messages to pass from producer to consumer directly (after the messages have been persisted), but switches back to using cursors if the consumer(s) fall behind.

When Message Consumers are both active and fast - keeping up with the Message Producer

![](/img/DispatchFastConsumers.png)  

If a Consumer becomes active after messages are pending from the store for it, or it's slower than the producer, then messages are paged in to the dispatch queue from a pending cursor: 

![](/img/DispatchSlowConsumers.png)

### Types of Cursor

The default message cursor type in ActiveMQ 5.0 is Store based.  It behaves as above. There are two additional types of cursor that could be used: **VM Cursor** and **File based Cursor**, described below.

#### VM Cursor

The VM Cursor is how ActiveMQ 4.x works: references to a message are held in memory, and passed to the dispatch queue when needed. This can be very fast, but also has the downside of not being able to handle very slow consumers or consumers that have been inactive for a long time: ![](/img/VMCursor.png)

#### File based Cursor

The File based Cursor is dervied from the VM Cursor.  When memory in the broker reaches its limit, it can page messages to temporary files on disk. This type of cursor can be used when the message store might be relatively slow, but consumers are generally fast. By buffering to disk, it allows the message broker to handle message bursts from producers without resorting to paging in from slow storage: 

![](/img/FileCursor.png)

#### Paging for Non-Persistent Messages

The store based cursor also handles cursors for non-persistent messages, which are not stored in the message store. Non-persistent messages are passed directly to the cursor, so the store based cursor embeds a file based cursor just for these types of messages: 

![](/img/NonPersistentMsgs.png)

### Configuring Cursors

By default, Store based cursors are used, but it is possible to configure different cursors depending on the destination.

#### Topic subscribers

For Topics there is a dispatch queue and pending cursor for every subscriber.  It's possible to configure different policies for durable subscribers and transient subscribers - e.g:
```
<destinationPolicy>
      <policyMap>
        <policyEntries>
          <policyEntry topic="org.apache.>" producerFlowControl="false" memoryLimit="1mb">
            <dispatchPolicy>
              <strictOrderDispatchPolicy />
            </dispatchPolicy>
            <deadLetterStrategy>
              <individualDeadLetterStrategy  topicPrefix="Test.DLQ." />
            </deadLetterStrategy>
            <pendingSubscriberPolicy>
            	<vmCursor />
            </pendingSubscriberPolicy>
            <pendingDurableSubscriberPolicy>
                <vmDurableCursor/>
            </pendingDurableSubscriberPolicy>
          </policyEntry>
        </policyEntries>
      </policyMap>
</destinationPolicy>
```

Valid Subscriber types are **_vmCursor_** and **_fileCursor._** The default is the store based cursor.  

Valid Durable Subscriber cursor types are **_storeDurableSubscriberCursor_**, **_vmDurableCursor_** and **_fileDurableSubscriberCursor._** The default is the store based cursor

#### Queues

For Queues there is a single dispatch Queue and pending Queue for every destination, so configuration is slightly different:
```
<destinationPolicy>
      <policyMap>
        <policyEntries>
          <policyEntry queue="org.apache.>">
            <deadLetterStrategy>
              <individualDeadLetterStrategy queuePrefix="Test.DLQ."/>
            </deadLetterStrategy>
            <pendingQueuePolicy>
            	<vmQueueCursor />
            </pendingQueuePolicy>
          </policyEntry>
        </policyEntries>
      </policyMap>
 </destinationPolicy>
```
Valid Queue cursor types are **_storeCursor_**, **_vmQueueCursor_** and **_fileQueueCursor._** The default is the store based cursor

### See Also

*   [Producer Flow Control](producer-flow-control)

