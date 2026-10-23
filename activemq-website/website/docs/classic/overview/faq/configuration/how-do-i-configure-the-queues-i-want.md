---
title: "How do I configure the queues I want"
slug: /how-do-i-configure-the-queues-i-want
---

The license could not be verified: License Certificate has expired!

With ActiveMQ there is no real resaon to explicitly setup/configure the queues you are gonna need. If you try to publish or subscribe from any queue or topic it will be silently created on the fly.

Sometimes people put destinations into JNDI so that they can be pulled out by their application without needing to know the real, physical queue/topic name. This is fine, you can just add the ActiveMQQueue and ActiveMQTopic objects into JNDI (or any configuration mechanism you wish).

Also if you use the ActiveMQInitialContextFactory, then you can explicitly configure which destinations appear in the context using the jndi.properties approach. For more information please refer to the [JNDI Support](jndi-support)

