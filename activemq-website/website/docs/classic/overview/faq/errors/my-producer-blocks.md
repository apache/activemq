---
title: "My producer blocks"
slug: /my-producer-blocks
---

What can I do if my producer blocks sending a message?
------------------------------------------------------

This relates to [Producer Flow Control](producer-flow-control).

### Active 4.x

In ActiveMQ 4.x, all in transit messages are held in memory. If you have a slow consumer, to avoid exausting the JVM memory and getting an out of memory error, ActiveMQ has a configurable limit of how many in transit messages it will hold. Once it is reached, producers will be slowed down / blocked.

A quick fix is just to bump up the setting in your **usageManager** in your [Xml Configuration](xml-configuration). The default that ships with ActiveMQ is pretty low, only 20Mb of RAM, so try setting it to something large, say 512Mb.

An alternative approach is to set individual usageManager limits on different destinations and for producers versus consumers.

You might also want to use the [JMX](jmx) support or [Web Console](web-console) to look at what destinations are being used and what producers & consumers you have to see whats going on.

### Active 5.x and Later

As of 5.x of ActiveMQ there is support for [Message Cursors](message-cursors) which by default move messages out of memory and onto disk. So this problem will not be seen unless you fill up the max allocated disk space for message storage which typically is an order of magnitude larger.

### See Also

*   [Producer Flow Control](producer-flow-control)
*   [Message Cursors](message-cursors)

