---
title: "ActiveMQ InactivityMonitor"
description: "How the ActiveMQ Classic InactivityMonitor detects dead connections using KeepAliveInfo, and how to tune or disable it with maxInactivityDuration."
slug: /activemq-classic-inactivitymonitor
---


ActiveMQ InactivityMonitor
==================================

The ActiveMQ InactivityMonitor is an active thread that checks the connection is still active and if it suspects the connection is not functioning correctly, it closes the connection.

Connections are monitored by:

*   Ensuring data is read from the connection during the specified time period (Max Inactivity Duration).
*   Writing a `KeepAliveInfo` message to the connection if no **normal** activemq traffic is sent across the connection during the specified time period.

Each connection has two InactivityMonitors associated, one on each end of the connection. The InactivityMonitor expects to receive data on the connection during a specified time period. If **normal** ActiveMQ traffic has not been sent across the connection during that period, it expects to receive a `KeepAliveInfo` message sent by the InactivityMonitor on the other end of the connection.

Using the default values; if no data has been written or read from the connection for 30 seconds, the InactivityMonitor kicks in. The InactivityMonitor throws an `InactivityIOException` and shuts down the transport associated with the connection. This results in the following `DEBUG` logging:
```
2012-06-26 17:13:55,712 | DEBUG | 30000 ms elapsed since last read check. | org.apache.activemq.transport.AbstractInactivityMonitor | InactivityMonitor ReadCheck
2012-06-26 17:13:55,712 | DEBUG | No message received since last read check for tcp:///127.0.0.1:52659! Throwing InactivityIOException. | org.apache.activemq.transport.AbstractInactivityMonitor | InactivityMonitor ReadCheck
2012-06-26 17:13:55,714 | DEBUG | Transport Connection to: tcp://127.0.0.1:52659 failed: 
org.apache.activemq.transport.InactivityIOException: Channel was inactive for too (>30000) long: tcp://127.0.0.1:52659 |
org.apache.activemq.broker.TransportConnection.Transport | InactivityMonitor Async Task: 
java.util.concurrent.ThreadPoolExecutor$Worker@6a346239
org.apache.activemq.transport.InactivityIOException: Channel was inactive for too (>30000) long: tcp://127.0.0.1:52659
    at org.apache.activemq.transport.AbstractInactivityMonitor$4.run(AbstractInactivityMonitor.java:187)
    at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
    at java.lang.Thread.run(Thread.java:680)
```

### Advanced InactivityMonitor Configuration

There are some configuration options to further fine tune the `InactivityMonitor`. Note, for most use cases the default values are just fine.

Parameter|Default Value|Description
---|---|---
`wireFormat.maxInactivityDuration`|`30000`|Timeout, in milliseconds, after which the connection is closed by the broker if no data has been received.
`wireFormat.maxInactivityDurationInitalDelay`|`10000`|Specifies the maximum delay, in milliseconds, before connection inactivity monitoring is started. This can prove useful if a broker is under load with many connections being created concurrently.
`transport.useInactivityMonitor`|`true`|A value of `false` disables the `InactivityMonitor` completely and connections will never time out.
`transport.useKeepAlive`|`true`|Determines if a `KeepAliveInfo` message should be sent on an idle connection to prevent it from timing out. Disabling the keep alive will still make connections time out when no data was received on the connection for the specified amount of time.

These parameters can be specified directly on the client side connection URL, e.g. `tcp://localhost:61616?wireFormat.maxInactivityDuration=30000`,** or on the broker's transport connector URL:
```
<transportConnectors>
  <transportConnector name="openwire" uri="tcp://0.0.0.0:61616?wireFormat.maxInactivityDuration=30000&amp;wireFormat.maxInactivityDurationInitalDelay=10000"/>
</transportConnectors>
```
### What happens if the `maxInactivityDuration` and `maxInactivityDurationInitalDelay` are set to different values on either side of the same connection?

At startup the InactivityMonitor negotiates the appropriate `maxInactivityDuration` and `maxInactivityDurationInitalDelay`. The shortest duration is taken for the connection.

### Can the InactivityMonitor on a connection be disabled?

Setting `transport.useInactivityMonitor=false` will disable the InactivityMonitor. Configuring `wireFormat.maxInactivityDuration=0` will achieve the same result.

### Potential Issues

[slow-networks-drop-large-messages](slow-networks-drop-large-messages)

