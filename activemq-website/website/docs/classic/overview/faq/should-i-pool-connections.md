---
title: "Should I pool connections"
slug: /should-i-pool-connections
---

Should I pool connections?
--------------------------

The Java documentation talks about [how to use JMS efficiently](how-do-i-use-jms-efficiently). So when using .NET should you worry about pooling?

Its worth thinking about - but it depends a little on your application. If its a client UI then just have a single connection and a few sessions and you'll be fine.

If its more of a server, processing messages efficiently in a highly concurrent manner with loads of threads, then some kinda pooling mechanism such as in Spring.NET is preferable than trying to write that your self; as pooling of messaging artefacts (connections, sessions, producers, consumers) is way more complex than pooling of say, JDBC connections.


