---
title: "Can you browse a topic"
slug: /can-you-browse-a-topic
---

You can browse queues, can you browse a topic?

No. But then consuming messages on a topic does not affect any other consumers, so you don't need to 'browse' per se, just subscribe.

i.e. browsing is necessary on queues as you wanna see what messages there are without removing them. For topics, everyone who's interested gets a copy of the message so just do a regular subscribe.

One nice to have feature would be to expose durable subscriptions as a logical queue, so you could browse outstanding messages on a durable subscription as if it were a queue (see [AMQ-25](https://issues.apache.org/activemq/browse/AMQ-25) to track this feature request)but its not in any way standard JMS.

