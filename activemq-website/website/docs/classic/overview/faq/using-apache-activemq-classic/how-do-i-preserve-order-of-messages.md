---
title: "How do I preserve order of messages"
slug: /how-do-i-preserve-order-of-messages
---

ActiveMQ will preserve the order of messages sent by a single producer to all consumers on a topic. If there is a single consumer on a queue then the order of messages sent by a single producer will be preserved as well.

If you have multiple consumers on a single queue the consumers will compete for messages and ActiveMQ will load balance across them, so order will be lost. For background on the issue and how it can be solved see

*   [Exclusive Consumer](exclusive-consumer) which allows only a single consumer to consume from the queue at once to preseve order
*   [Message Groups](message-groups) which splits the messages on a queue into parallel virtual exclusive queues to ensure that messages to a single message group (defined by the JMSXGroupID header) will have their order preserved but that different groups will be load balanced to different consumers.

