---
title: "I see NC_ client-ids, what does that mean"
slug: /i-see-nc-client-ids-what-does-that-mean
---

Durable subscription ClientIds and SubscriptionNames using the **NC** prefix are the result of durable subscriptions in a [Networks of Brokers](networks-of-brokers).  
When a durable subscription is being forwarded by a network connector (or demand forwarding bridge), the network durable subscription needs to outlive the subscription that created it. This is achieved by using a well known name for the clientId and subscriptionName that can be easily be mapped to the original subscription. The prefix NC_, and NC-DS_ are used, where NC denotes Network Connector and DS denotes Durable Subscription. The prefix is combined with the local broker name and the target destination.

The expectation is that the connectionId associated with these subscriptions can change on a reconnect, but the durable subsctiption remains. In this way, the durable subscription can continue to receive messages even if there is a network partition between the originating broker and the forwarding broker.

On a restart, the NC durable subscriptions are activated by default, to ensure that no messages are lost. If lost messages are acceptable, it is possible to ensure that the NC durable sub is activated dynamically, when the original subscription is again activated on the remote broker and propagated to the network connector. The network connector configuration 'dynamicOnly' attribute is used to control this behavior.

A NC durable subscription is only deleted when the original durable consumer is unsubscribed.

