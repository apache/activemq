---
title: "Title"
slug: /stomp-manual
---

       

Apollo 1.7.1 STOMP Protocol Manual
==================================

*   [Using the STOMP Protocol](#using-the-stomp-protocol)
*   *   [Stomp Protocol Options](#stomp-protocol-options)
    *   [Client Libraries](#client-libraries)
    *   [Connecting](#connecting)
    *   [Destination Types](#destination-types)
    *   [Topic Retained Messages](#topic-retained-messages)
    *   [Reliable Messaging](#reliable-messaging)
    *   [Message Expiration](#message-expiration)
    *   [Subscription Flow Control](#subscription-flow-control)
    *   [Topic Durable Subscriptions](#topic-durable-subscriptions)
    *   [Browsing Subscriptions](#browsing-subscriptions)
    *   [Queue Message Sequences](#queue-message-sequences)
    *   [Using Queue Browsers to Implement Durable Topic Subscriptions](#using-queue-browsers-to-implement-durable-topic-subscriptions)
    *   [Exclusive Subscriptions](#exclusive-subscriptions)
    *   [Message Groups](#message-groups)
    *   [Temporary Destinations](#temporary-destinations)
    *   [Destination Wildcards](#destination-wildcards)
    *   [Composite Destinations](#composite-destinations)
    *   [Message Selectors](#message-selectors)
    *   [Destination Name Restrictions](#destination-name-restrictions)

Using the STOMP Protocol
------------------------

Clients can connect to Apollo using the [STOMP](http://stomp.github.com/) protocol. STOMP provides an interoperable messaging wire level protocol that allows any STOMP clients can communicate with any STOMP message broker. It aims to provide easy and widespread messaging interoperability among many languages, platforms and brokers.

Apollo supports the following versions of the STOMP specification:

*   [STOMP 1.0](http://stomp.github.com/stomp-specification-1.0.html)
*   [STOMP 1.1](http://stomp.github.com/stomp-specification-1.1.html)
*   [STOMP 1.2](http://stomp.github.com/stomp-specification-1.2.html)

The specification is short and simple to read, it is highly recommend that users to get familiar with it before using one of the many available client libraries.

### Stomp Protocol Options

You can use the `stomp` configuration element within the `connector` element in the `apollo.xml` configuration file to change the default settings used in the STOMP protocol implementation. The `stomp` element supports the following configuration attributes:

*   `buffer_size` : How much each producer or subscription will buffer between the client and the broker. If not set, it will be auto tuned between `640k` and `20k` depending on the number of connections open on the broker.
*   `add_user_header` : Name of the header which will be added to every received message received. The value of the header will be set to the id of user that sent the message. Not set by default.
*   `add_timestamp_header` : Name of the header which will be added to every received message received. The value of the header will be set to the time the message was received. The time will be represented as the number of milliseconds elapsed since the UNIX epoch in GMT. Not set by default.
*   `add_redeliveries_header` : Name of the header which will be added to messages sent to consumers if the messages has been redelivered. The value of the header will be set to the number of times the message has been redeliverd. Not set by default.
*   `max_header_length` : The maximum allowed length of a STOMP header. Defaults to `10k`.
*   `max_headers` : The maximum number of allowed headers in a frame. Defaults to 1000.
*   `max_data_length` : The maximum size of the body portion of a STOMP frame.  
    Defaults to `100M`.
*   `die_delay` : The amount of time to delay in milliseconds after an `ERROR` message is sent to the client and the socket is closed.

The stomp configuration element can also be used to control how the destination headers are parsed and interpreted. The supported attributes are:

*   `queue_prefix` : Defaults to `/queue/`
*   `topic_prefix` : Defaults to `/topic/`
*   `path_separator` : Defaults to `.`
*   `destination_separator` : Defaults to `,`
*   `any_child_wildcard` : Defaults to `*`
*   `regex_wildcard_start` : Defaults to `{`
*   `regex_wildcard_end` : Defaults to `}`
*   `any_descendant_wildcard` : Defaults to `**`

It also supports nested `add_user_header` elements to more finely control how user headers are added to received STOMP messages. The `add_user_header` element should contain the name of the header to set on the STOMP message. It also supports the following attributes:

*   `separator` : If user has multiple principles which match, this separator will be used to delimit them in the header. If not set, then only the first matching principle will be set in the header.
*   `kind` : The principle kind to look for. Defaults to `*` (matches all principle kinds)

Example:

    
    <connector id="tcp" bind="tcp://0.0.0.0:61613">
      <stomp max_header_length="10000">
        <add_user_header separator=",">user</add_user_header>
      </stomp>
    </connector>
    

### Client Libraries

There are many open source STOMP clients for different platforms and languages. You can find a full listing of available clients at:

*   [`http://stomp.github.com/implementations.html#Clients`](http://stomp.github.com/implementations.html#Clients)

The Apollo distribution ships with an `examples` directory where you can find some simple examples of how to use some of those clients to send and receive messages from a broker instance.

This section will focus on clarifying the parts of the STOMP specification which allow servers to choose their behavior and to describe how to access Apollo specific features.

### Connecting

The default broker configuration secures access to the broker so that only the `admin` user can connect. The default password for the `admin` user is `password`.

Example STOMP frame to connect to the broker:

    CONNECT
    
    login:admin
    passcode:password
    
    ^@

STOMP 1.0 clients don't specify which virtual host they are connecting to so the broker connects those clients to the first virtual host defined in it's configuration. STOMP 1.1 clients must specify a virtual host when they connect. If no configured virtual host `host_name` matches the client's requested host, the connection is terminated with an ERROR. Therefore, it is critical that the virtual hosts configuration define all the possible host names that clients may connect to host with.

### Destination Types

Apollo supports three types of destinations, queues, topics, and durable subscriptions.

The difference between queues and topics is how messages are delivered to consumers. A queue will load balance it's messages over the connected subscribers so that only one subscriber gets a message. Topics replicate every message sent to it to all the connected subscribers. Queues hold on to unconsumed messages even when there are no subscriptions attached, while a topic will drop messages when there are no connected subscriptions.

A durable subscription allows you to create a subscription against a topic which will queue messages even after the client disconnects. Clients can reconnect and consume the queued message originating from the topic at a later time.

If you want to send or subscribe to a queue, topic, or durable subscription the STOMP destination should be prefixed with `/queue/`, `/topic/` or `/dsub/` respectively.

Example STOMP frame sending to a queue:

    SEND
    destination:/queue/a
    
    hello queue a
    ^@

### Topic Retained Messages

If a message sent to a Topic has the `retain:set` header, then the message will be 'remembered' by the topic so that if a new subscription arrives, the last retained message is sent to the subscription. For example if you want a topic to remember the last price published you can send a message that looks like:

    SEND
    destination:/topic/stock/IBM
    retain:true
    
    112.12
    ^@

You can also send a new message with the `retain:remove` header to have the topic forget about the last retained message.

Note: retained messages are not retained between broker restarts.

### Reliable Messaging

Apollo supports reliable messaging by allowing messages to be persisted so that they can be recovered if there is failure which kills the broker. Processing persistent messages has orders of magnitude more overhead than non-persistent variety. You should only use it if your application really needs it.

To have a message be persistent, the sender must add the `persistent:true` header to the `SEND` STOMP frame. Furthermore, if you are not sending the message in a transaction, it is recommend that you also add a `receipt` header. Once the broker responds with a `RECEIPT` frame correlated to the send, you are guaranteed the broker will not drop the message even if a failure occurs.

Example:

    SEND
    destination:/queue/a
    receipt:001
    
    hello queue a
    ^@

The client should not consider the send to have succeeded until he receives the correlated `RECEIPT` frame from the server. Example:

    RECEIPT
    receipt-id:001
    
    ^@

It is important to note that to do reliable messaging, your system will be prone to receive duplicate messages. For example, if your sending application dies before it receives the `RECEIPT` frame, then when it starts up again will probably try to send the same message again which may cause a duplicate message to get placed on the broker.

You should only use subscribers which use the `client` or `client-individual` ack mode to consume reliable messages. Any messages on a queue delivered to a client which have not been acked when the client disconnects will get redelivered to another subscribed client.

### Message Expiration

Apollo supports expiring old messages. Unconsumed expired messages are automatically removed from the queue. There's two way to specify when the message will be expired. Y

The first way to configure the expiration is by setting the `expires` message header. The expiration time must be specified as the number of milliseconds since the Unix epoch.

Example:

    SEND
    destination:/queue/a
    expires:1308690148000
    
    this message will expire on Tue Jun 21 17:02:28 EDT 2011
    ^@

The first way to configure the expiration is by setting the `ttl` message header. The ttl will be intereted to mean the number of milliseconds from when the server receives the message. The broker will add an `expires` header to the message on your behalf.

Example:

    SEND
    destination:/queue/a
    ttl:2000
    
    This message will expire in 2 seconds.
    ^@

### Subscription Flow Control

You can add a `credit` header to the `SUBSCRIBE` frame to control the flow of messages delivered to the client. Each subscription maintains 2 credit windows which allows you to control the flow of messages either based on number of messages sent or total number of content bytes delivered to the client. The content bytes are the body of the frame which is usually also set on the `content-length` header. If either of the two credit windows have a positive value, the server will deliver messages to the client.

The `credit` header value is expected to use the `count[,size]` syntax where:

*   count: initial setting of the credit window tracking the remaining number of messages that can be sent. Must be specified.
*   size: initial setting of the credit window tracking the remaining number of content bytes that can be sent. Defaults to the receive buffer size of the TCP socket which is typically 65536 bytes.

If the `credit` header is not specified it has the same effect as if it had been set to `credit:655360,655360`. This setting allows the broker to optimally stream many small messages to the client or without overloading the clients processing buffers.

As messages are sent from the server to the client, the credit windows are reduced by the corresponding amount. When the client send `ACK` frames to the server, the credit windows are incremented by the number of messages and content sizes corresponding to the messages that were acknowledged.

Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/foo
    credit:5,0
    
    ^@

The above example would cause the subscription to only receive at most 5 messages if the client is not sending any `ACK` messages back to the server.

If you need to receive more messages without acknowledging them than the size of the credit window, you should use transactions and transactionally `ACK` messages as they are received. A transactional ACK increases the credit window without actually consuming the messages. If the transaction is aborted subsequent acknowledgements of a previously acknowledged message does not increase the credit window.

### Topic Durable Subscriptions

A durable subscription is a queue which is subscribed to a topic so that even if the client which created the durable subscription is not online, he can still get a copy of all the messages sent to the topic when he comes back online. Multiple clients can subscribe to the same durable subscription and since it's backed by a queue, those subscribers will have the topic's messages load balanced across them.

To create or reattach to a a durable subscription with STOMP, you uniquely name the durable subscription using the `id` header on the `SUBSCRIBE` frame and also adding a `persistent:true` header. Example:

    SUBSCRIBE
    id:mysub
    persistent:true
    destination:/topic/foo
    
    ^@

A standard `UNSUBSCRIBE` frame does not destroy the durable subscription, it only disconnects the client from the durable subscription. To destroy a durable subscription, you must once again add `persistent:true` header to the `UNSUBSCRIBE` frame. Example:

    UNSUBSCRIBE
    id:mysub
    persistent:true
    
    ^@

If the durable subscription already exists you can address it directly using `/dsub/` prefix on the `destination` header. For example, send a message to the previously created `mysub` durable subscription, you send the following STOMP frame:

    SEND
    destination:/dsub/mysub
    
    hello durable sub!
    ^@

Similarly, you can also subscribe to the subscription in the same way:

    SUBSCRIBE
    id:0
    destination:/dsub/mysub
    
    ^@

Unlike typical STOMP subscriptions id's which are local to the STOMP client's connection, the durable subscription id's are global across a virtual host. If two different connections use the same durable subscription id, then messages from the subscription will get load balanced across the two connections. If the second connection uses a different `destination` or `selector` header, then updates the original subscription, and the original connection will subsequently only receive messages matching the updated destination or selector.

### Browsing Subscriptions

A normal subscription on a queue will consume messages so that no other subscription will get a copy of the message. If you want to browse all the messages on a queue in a non-destructive fashion, you can create browsing subscription. Browsing subscriptions also works with durable subscriptions since they are backed by a queue. To make a a browsing subscription, just add the `browser:true` header to the `SUBSCRIBE` frame. For example:

    SUBSCRIBE
    id:mysub
    browser:true
    destination:/queue/foo
    
    ^@

Once the broker sends a browsing subscription the last message in the queue, it will send the subscription a special “end of browse” message to indicate browsing has completed and that the subscription should not expect any more messages. The “end of browse” message will have a `browser:end` header set. Example:

    MESSAGE
    subscription:mysub
    destination:
    message-id:
    browser:end
    
    ^@

If you want the browsing subscription to remain active and continue to listen for message once the last message on the queue is reached, you should add the `browser-end:false` header to the `SUBSCRIBE` frame. When the `browser-end:false` header is added the subscription will not be sent the “end of browse” message previously described.

### Queue Message Sequences

As messages are added to a queue in a broker, they are assigned an incrementing sequence number. Messages delivered to subscribers can be updated to include the sequence number if the `include-seq` header is added to the `SUBSCRIBE` frame. This header should be set to a header name which will be added messages delivered to hold value of the sequence number.

Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/foo
    include-seq:seq
    
    ^@

Then you can expect to receive messages like:

    MESSAGE
    subscription:mysub
    destination:/queue/foo
    seq:1
    
    Hello
    ^@
    MESSAGE
    subscription:mysub
    destination:/queue/foo
    seq:2
    
    World
    ^@

Furthermore, you can configure the `SUBSCRIBE` frame so that the subscription only receives messages that have a sequence id that is equal to or greater than a requested value by using the `from-seq` header. Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/foo
    from-seq:10
    
    ^@

If the `from-seq` is set to `-1`, then the subscription will receive messages from the tail of the queue. In other words, it will only receive new messages sent to the queue.

Note: You can only use the `from-seq` header with normal destinations. If you attempt to use it with a wildcard or composite destination then the connection will be closed due to invalid usage.

### Using Queue Browsers to Implement Durable Topic Subscriptions

You can use queue browsers with consumer side message sequence tracking to achieve the same semantics as durable topics subscriptions but with a better performance profile. Since browsers do not delete messages from a queue, when you use multiple browsers against one queue you get the same broadcast effects that a topic provides.

In this approach the subscribing application keeps track of the last sequence number processed from the subscription. The sequence number is typically stored as part of the unit of work which is processing the message. The subscription can use the default auto acknowledge mode but still get 'once and only once' delivery guarantees since:

*   consuming application records the last message sequence that was processed
*   message are not deleted when delivered to the subscriber
*   on restart consuming application continues receiving from the queue for the last sequence that it received.

The `SUBSCRIBE` frame used to create the browser should add the `include-seq`, `from-seq`, and `browser-end` headers so that they can resume receiving messages from the queue from the last known sequence. If you are starting a new consumer that does not have a last processed sequence number, you can either set `from-seq` to:

*   `0` to start receiving at the head of the queue which sends the subscription a copy of all the messages that are currently queued.
*   `-1` to start receiving at the tail of the queue which to skips over all the message that exist in the queue so that the subscription only receives new messages.

Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/foo
    browser:true
    browser-end:false
    include-seq:seq
    from-seq:0
    
    ^@

Since this approach does not consume the messages from the queue, you should either:

*   Send messages to the queue with an expiration time so that they are automatically delete once the expiration time is reached.
*   Periodically run a normal consumer application which can cursor the queue and delete messages are are deemed no longer needed.

### Exclusive Subscriptions

We maintain the order of messages in queues and dispatch them to subscriptions in order. However if you have multiple subscriptions consuming from the same queue, you will loose the guarantee of processing the messages in order; since the messages may be processed concurrently on different subscribers.

Sometimes it is important to guarantee the order in which messages are processed. e.g. you don't want to process the update to an order until an insert has been done; or to go backwards in time, overwriting an newer update of an order with an older one etc.

So what folks have to do in clusters is often to only run one consumer process in a cluster to avoid loosing the ordering. The problem with this is that if that process goes down, no one is processing the queue any more, which can be problem.

Apollo supports exclusive subscriptions which avoids the end user having to restrict himself to only running one process. The broker will pick a single subscription to get all the messages for a queue to ensure ordering. If that subscription fails, the broker will auto failover and choose another subscription.

An exclusive subscription is created by adding a `exclusive:true` header to the `SUBSCRIBE` frame. Example:

    SUBSCRIBE
    id:mysub
    exclusive:true
    destination:/queue/foo
    
    ^@

### Message Groups

Message Groups are an enhancement to the Exclusive Consumer feature to provide

*   guaranteed ordering of the processing of related messages across a single queue
*   load balancing of the processing of messages across multiple consumers
*   high availability / auto-failover to other consumers if a JVM goes down

Message Groups are logically like a parallel Exclusive Consumer. Rather than all messages going to a single consumer, the stomp `message_group` header is used to define which message group the message belongs to. The Message Group feature then ensures that all messages for the same message group will be sent to the currently assigned consumer for the group. The assigned consumer consumer for a message group may change but not before all messages sent to the previous consumer are acked or if the consumer is disconnected.

Another way of explaining Message Groups is that it provides sticky load balancing of messages across consumers; where the message group value is kinda like a HTTP session ID or cookie value and the message broker is acting like a HTTP load balancer.

Here is an example message with the message group set:

    MESSAGE
    destination:/queue/PO.REQUEST
    message_group:hiram
    
    PO145
    ^@

The broker uses consistent hashing to map message groups to consumers. When you add another subscription to a queue, the broker will first wait for messages sent to previous subscriptions to be processed and then the broker rebalances the message groups across the attached consumers.

### Temporary Destinations

Temporary destinations are typically used to receive response messages in a request/response messaging exchange. A temporary destination can only be consumed by a subscription created on the connection which is associated with the temporary destination. Once the connection is closed, all associated temporary destinations are removed. Temporary destinations are prefixed with:

*   `/temp-queue/` \- For temporary queues. Has the same delivery semantics as queues.
*   `/temp-topic/` \- For temporary topics. It has the same delivery semantics of topics.

In a request/response scenario, you would first subscribe to the temporary topic:

    SUBSCRIBE
    id:mysub
    destination:/temp-queue/example
    
    ^@

Then you would send a request with the reply-to header set to the temporary destination. Example:

    SEND
    destination:/queue/PO.REQUEST
    reply-to:/temp-queue/example
    
    PO145
    ^@

The consumer receiving the request will receive a message similar to:

    MESSAGE
    subscription:foo
    reply-to:/queue/temp.default.23.example
    destination:/queue/PO.REQUEST
    reply-to:/temp-queue/example
    
    PO145

Notice that the reply-to` header value is updated from a temporary destination name to normal destination name. The subscription servicing he requests should respond to the updated destination value (`/queue/temp.default.23.example` in the example above).

Temporary destination names actually map to normal queues and topics. They just have a `temp.<broker_id>.<connection_id>.` prefix. Any destination which starts with `temp.` has a security policy which only allows the connection which created it to subscribe from it. These destinations are also auto deleted once the connection is closed.

### Destination Wildcards

We support destination wildcards to easily subscribe to multiple destinations with one subscription. This concept has been popular in financial market data for some time as a way of organizing events (such as price changes) into hierarchies and to use wildcards for easy subscription of the range of information you're interested in.

*   `.` is used to separate names in a path
*   `*` is used to match any name in a path
*   `{regex}` is used to match a path name against a regular expression.
*   `**` is used to recursively match path names

For example imagine you are sending price messages from a stock exchange feed. You might use some kind of destination naming conventions such as:

*   `/topic/PRICE.STOCK.NASDAQ.IBM` to publish IBM's price on NASDAQ and
*   `/topic/PRICE.STOCK.NYSE.SUNW` to publish Sun's price on the New York Stock Exchange

A subscriber could then use exact destinations to subscribe to exactly the prices it requires. Or it could use wildcards to define hierarchical pattern matches to the destinations to subscribe from.

For example using the example above, these subscriptions are possible

*   `/topic/PRICE.**` : Any price for any product on any exchange
*   `/topic/PRICE.STOCK.**` : Any price for a stock on any exchange
*   `/topic/PRICE.STOCK.NASDAQ.*` : Any stock price on NASDAQ
*   `/topic/PRICE.STOCK.*.IBM` : Any IBM stock price on any exchange
*   `/topic/PRICE.STOCK.*.I*` : Any stock price starting with 'I' on any exchange
*   `/topic/PRICE.STOCK.*.*{[0-9]}` : Any stock price that ends in a digit on any exchange

Destination wildcards can only be used in a SUBSCRIBE frame.

### Composite Destinations

You can use composite destinations to send or subscribe to multiple destinations at one time. You use separator of `,` between destination names. For example, to send one message to 2 queues and 1 topic:

    SEND
    destination:/queue/a,/queue/b,/topic/c
    
    Composites rock!
    ^@

### Message Selectors

Message selectors allow a subscription to only receive a subset of the messages sent to a destination. The selector acts like a filter which is applied against the message properties and only those messages which match pass through to the subscription.

Selectors are defined using SQL 92 syntax and typically apply to message headers; whether the standard properties available on a JMS message or custom headers you can add via the JMS code.

Here is an example:

    type = 'car' AND color = 'blue' AND weight > 2500

To create a subscription with a message selector, you set the `selector` header in the STOMP `SUBSCRIBE` frame to the desired selector. Example:

    SUBSCRIBE
    id:sub0
    selector:type = 'car' AND color = 'blue' AND weight > 2500
    destination:/topic/foo
    
    ^@

### Destination Name Restrictions

Destination names are restricted to using the characters `a-z`, `A-Z`, `0-9`, `_`, `-` `%`, `~`, `:`, ' ', '(', ')' or `.` in addition to composite separator `,` and the wild card `*`. Any other characters must be UTF-8 and then URL encoded if you wish to preserve their significance.