---
title: "OpenWire Support"
sidebar_position: 5
---
## ActiveMQ-CPP OpenWire Support

[OpenWire](http://activemq.apache.org/openwire.html) is the native protocol used by the [ActiveMQ 5.x broker](http://activemq.apache.org/). As of version 2.0, ActiveMQ-CPP supports the OpenWire protocol, with a few exceptions.

* ObjectMessage - We cannot reconstruct the object(s) contained in an ObjectMessage in C++, so if your application is subscribed to a queue or topic that has an ObjectMessage sent to it, you will receive the message but will not be able to extract an Object from it.
