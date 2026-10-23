---
title: "Exclusive Consumer"
slug: /exclusive-consumer
---


### Background

We maintain the order of messages in queues and dispatch them to consumers in order. However if you have multiple JMS Sessions and MessageConsumer instances consuming from the same queue (whether in the same JVM or not), you will loose the guarantee of processing the messages in order; since the messages will be processed concurrently in different threads.

Sometimes its important to guarantee the order in which messages are processed. e.g. you don't want to process the update to an order until an insert has been done; or to go backwards in time, overwriting an newer update of an order with an older one etc.

So what folks have to do in J2EE clusters is often to _pin_ one particular JVM in the cluster to have _one_ consumer on the queue to avoid loosing ordering. The problem with this is that if the particular pinned JVM goes down, no one is processing the queue any more, which can be problem.

### Exclusive Consumer

We have a new feature in 4.x called Exclusive Consumer or Exclusive Queues which avoids the end user having to _pin_ anything. The broker will pick a single MessageConsumer to get all the messages for a queue to ensure ordering. If that consumer fails, the broker will auto failover and choose another consumer.

So the effect is a heterogeneous J2EE cluster where each JVM has the same setup and configuration; the broker is choosing one consumer to be the _master_ and send all the messages to it in order until it dies; then you get immediate failover to another consumer.

For those who've struggled with pinning JMS consumers in J2EE clusters you'll immediately realize how useful this is to making clustered, high available distributed services.

### Example

An Exclusive Consumer is created using [Destination Options](destination-options) as follows:
```
queue = new ActiveMQQueue("TEST.QUEUE?consumer.exclusive=true");
consumer = session.createConsumer(queue);
```
