---
title: "ZeroConf"
description: "ZeroConf (Bonjour) service discovery support in ActiveMQ Classic, implemented with jmDNS for broker discovery and deprecated for removal from ActiveMQ 5.8."
slug: /zeroconf
---


ZeroConf
--------

[ZeroConf](http://www.zeroconf.org/) is a standard service discovery mechanism. ZeroConf is used on Apple's OS X; which used to call it Rendezvous but now calls it Bonjour.

Support for [ZeroConf](zeroconf) is **deprecated** and scheduled to be removed from ActiveMQ 5.8 onwards.

We use the [jmDNS](http://jmdns.sf.net/) project to implement the Zeroconf specification to detect services. This means other Zeroconf  
based tools can be used in conjunction with this discovery agent.

You can use ZeroConf with ActiveMQ's [Discovery](discovery) mechanism. For more details see

*   [Discovery](discovery)
*   ZeroConf Transport Reference

