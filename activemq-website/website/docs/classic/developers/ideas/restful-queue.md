---
title: "RESTful Queue"
slug: /restful-queue
---


RESTful Queue
-------------

This document is intended to document the ideal RESTful interface to message queues in light of

*   [the discussion on rest-discuss](http://tech.groups.yahoo.com/group/rest-discuss/message/8955)
*   [Atom Publishing Protocol](http://bitworking.org/projects/atom/draft-ietf-atompub-protocol-17.html) (APP)
*   [httplr](http://www.dehora.net/doc/httplr/draft-httplr-01.html)

### Issues with Queues and REST

One of the main issues with making a truly RESTful API to a message queue is that a message queue is essentially a load balancer, so each consumer of a queue sees essentially a different queue; as only one consumer gets a copy of each message.

Also if a consumer goes away, the messages that were associated with that consumer need to be re-assigned to another consumer. So the main tricky part really is the operation for a consumer to find out what messages it may consume.

### Queue administration

This section deals with the general browsing and creation/deletion of queues.

#### Browse available queues

To browse the queues, they are a hierarchial structure usually, so lets browse them like any APP collection
```
GET /queues
-------------------->

200 OK
Atom Feed with one entry per directory/category/queue
<--------------------
```

#### Browse a queue's messages
```
GET /queues/uk/products/books/computers
-------------------->

200 OK
Atom Feed with one entry per pending message
<--------------------
```
Note that we could expose the queues as a tree, for example
```
GET /queues/uk/products
-------------------->

200 OK
Atom Feed with one entry per queue in uk.products.* together with any child catgory/directory
<--------------------
```

#### Creating a queue

Creating a queue is typically idempotent; since really you are just creating a name that folks can post to.
```
POST /queues
-------------------->

201 OK
Location: someUniqueUrlForTheNewQueueToBePostedTo
<--------------------
```

#### Deleting a queue
```
DELETE /queues/foo.bar
-------------------->

200 OK
<--------------------
```
### Send to queue

Sending to queue is the usual APP-style double request; one to get the unique URI to post to and secondly to do the actual post...
```
POST /queues/foo.bar
-------------------->

201 OK
Location: someUniqueUrlForTheNewMessageToBePostedTo
<--------------------
```
The client can then repeatedly POST the mesage to someUniqueUrlForTheNewMessageToBePostedTo until it gets a 200 OK which avoids duplicates.

#### Optional alternative for single request

If a smart client is capable of generating a system wide unique GUID (id) for the message, the server side could ignore duplicates. So posting to queue could be done without the double-request approach above
```
POST /queues/foo.bar?guid=clientGeneratedUniqueId
-------------------->

200 OK
<--------------------
```

### Consuming from a queue

As described above, this is the tricky part of mapping message queues to REST.

So we introduce a resource for a _subscription_ to a queue. The subscription remains active until some timeout value - so a subscription is a sort of lease.

When a subscription is created it can be given a variety of different pieces of data such as

*   the [prefetch buffer](what-is-the-prefetch-limit-for)
*   what destinations it applies to (for example we could use wildcards)
*   whether a selector/filter is applied

#### Create a subscription
```
POST /subscriptions
-------------------->

201 OK
Location: subscriptionUri
<--------------------
```
The actual subscription data could be form encoded key/value pairs.

#### Deleting a subscription

Good clients will delete subscriptions when they are no longer required; though they do timeout eventually.
```
DELETE subscriptionUri
-------------------->

200 OK
<--------------------
```

#### Consuming messages

You consume messages by browsing the subscription (like any APP resource) then DELETEing the message when you have finished processing it.
```
GET subscriptionUri
-------------------->

200 OK
Atom Feed with one entry per message associated to this subscription
<--------------------
```
Then to acknowledge a particular message has been processed
```
DELETE messageUri
-------------------->

200 OK
<--------------------
```

Deltas with APP
---------------

Almost all of the above could be just pure APP really. The only real difference is that each consumer has its own feed of messages which are to be consumed.

In ActiveMQ's case, we use a [prefetch value per consumer](what-is-the-prefetch-limit-for) to define how many messages each consumer gets in its buffer, before messages must be acknowledged to get more messages.

So the idea is that we have a per-consumer feed which is created; it can then be browsed (by anyone with sufficient karma).

