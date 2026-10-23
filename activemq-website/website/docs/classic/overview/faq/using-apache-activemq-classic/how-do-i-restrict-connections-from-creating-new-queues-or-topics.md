---
title: "How do I restrict connections from creating new queues or topics"
slug: /how-do-i-restrict-connections-from-creating-new-queues-or-topics
---

How do I restrict connections from creating new queues or topics?
-----------------------------------------------------------------

As is described in [How do I create new destinations](how-do-i-create-new-destinations) there is no need to create all the destinations up front, you can let the broker create them on the fly.

However if you don't want this behaviour, or wish to restrict this behaviour to certain topic or queue [Wildcards](wildcards) (areas of the queue or topic name space) then you can use the [Security](security) plugins to disallow the **admin** role on whatever areas of the queue and topic namespace you wish

