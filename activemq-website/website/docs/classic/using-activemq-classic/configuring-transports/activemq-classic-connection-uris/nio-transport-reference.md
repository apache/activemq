---
title: "NIO Transport Reference"
description: "Use the nio:// transport in ActiveMQ Classic for scalable server-side connections, including +nio protocol variants and tuning SelectorManager thread pools."
slug: /nio-transport-reference
---


NIO Transport is very similar to the regular [TCP transport](tcp-transport-reference). The difference is that it is implemented using NIO API which can help with performance and scalability. NIO is a server side transport option only. Trying to use it on the client side will instantiate the regular TCP transport.

#### Configuration Syntax
```
nio://hostname:port?key=value
```
Configuration options are the same as for the [TCP transport](tcp-transport-reference).

Note that the original NIO transport is a replacement for the tcp transport that uses OpenWire protocol. Other network protocols, such AMQP, MQTT, Stomp, etc also have their own NIO transport implementations. It configured usually, by adding "+nio" suffix to the protocol prefix, like
```
mqtt+nio://localhost:1883
```
All protocol specific configuration should be applicable to the NIO version of the transport as well.

### Tuning NIO transport thread usage

One of the main advantages of using NIO instead of the regular versions of the transport is that it can scale better and support larger number of connections. The main limit in this scenario is the number of threads the system in using. In blocking implementations of the transports, one thread is used per connection. In the NIO implementation, there's a shared pool of threads that will take the load, so that number of connections are not directly related to the number of threads used in the system.

You can tune the number of threads used by the transport using the following system properties (available since **5.15.0**)

Property|Default value|Description
---|---|---
org.apache.activemq.transport.nio.SelectorManager.corePoolSize|10|The number of threads to keep in the pool, even if they are idle
org.apache.activemq.transport.nio.SelectorManager.maximumPoolSize|1024|The maximum number of threads to allow in the pool
org.apache.activemq.transport.nio.SelectorManager.workQueueCapacity| 0| The max work queue depth before growing the pool
org.apache.activemq.transport.nio.SelectorManager.rejectWork|false|Allow work to be rejected with an IOException when capacity is reached such that existing QOS can be preserved

If you want to scale your broker to support thousands of connections to it, you need to first find the limits of number of threads JVM process is allowed to create. Then you can set these properties to some value below that (broker need more threads to operate normally). For more information on thread usage by destinations and how to limit those, please take a look at [Scaling Queues](scaling-queues) or [this configuration file](http://svn.apache.org/repos/asf/activemq/trunk/assembly/src/sample-conf/activemq-scalability.xml). For example you can add the following
```
ACTIVEMQ_OPTS="$ACTIVEMQ_OPTS -Dorg.apache.activemq.transport.nio.SelectorManager.corePoolSize=2000 -Dorg.apache.activemq.transport.nio.SelectorManager.maximumPoolSize=2000 -Dorg.apache.activemq.transport.nio.SelectorManager.workQueueCapacity=1024"
```
to the startup script (`${ACTIVEMQ_HOME}/bin/env` for example) to have a constant pool of 2000 threads handling connections. With the setting like this, the broker should be able to accept the number of connections up to the system limits. Of course, accepting connections is just one part of the story, so there are other limits to vertically scaling the broker.

