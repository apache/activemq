---
title: "Multicast Transport Reference"
description: "Connect to ActiveMQ Classic over multicast://address:port, with options for group, datagramSize, timeToLive and loopBackMode, plus multicast://default examples."
slug: /multicast-transport-reference
---


### The Multicast Transport

The Multicast transport allows clients to connect to a remote ActiveMQ broker using multicast

Note that by default Multicast is not reliable; datagrams can be lost so you should add a reliability layer to ensure the JMS contract can be implemented on a non-reliable transport.

#### Configuration Syntax
```
multicast://address:port?transportOptions
```

##### Transport Options

Option Name|Default Value|Description
---|---|---
group|default|specify a unique group name that can segregate multicast traffic
minmumWireFormatVersion|0|The minimum version wireformat that is allowed
trace|false|Causes all commands that are sent over the transport to be logged
useLocalHost|true|When true, it causes the local machines name to resolve to "localhost".
datagramSize|4 * 1024|Specifies the size of a datagram
timeToLive|-1|The time to live of datagrams. Set greater than 1 to send packets beyond the local network. (**NOTE:** This won't work for IPv4 addresses without setting the property `java.net.preferIPv4Stack=true`. For more info see the [IPv6 User Guide for JDK](http://java.sun.com/j2se/1.5.0/docs/guide/net/ipv6_guide/index.html))
loopBackMode|false|Should loopback mode be used
wireFormat|default|The name of the WireFormat to use
wireFormat.*||All the properties with this prefix are used to configure the wireFormat. See [Configuring Wire Formats](configuring-wire-formats) for more information

##### Example URIs

To use the default value of "multicast://239.255.2.3:6155" with a default multicast group name of "default" from [MulticastDiscoveryAgent](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/transport/discovery/multicast/MulticastDiscoveryAgent.html#DEFAULT_DISCOVERY_URI_STRING) use
```
multicast://default 
```
To specify the IP address and port use
```
multicast://224.1.2.3:6255
```
To specify a multicast group name other than the default name of "default" use
```
multicast://224.1.2.3:6255?group=mygroupname
```
