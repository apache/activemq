---
title: "AUTO"
slug: /auto
---

Starting with version 5.13.0, ActiveMQ supports wire format protocol detection.   OpenWire, STOMP, AMQP, and MQTT can be automatically detected.  This allows one transport to be shared for all 4 types of clients.

### Enabling AUTO over TCP

To configure ActiveMQ auto wire format detection over a TCP connection use the `auto` transport prefix. For example, add the following transport configuration in your XML file:
```
<transportConnector name="auto" uri="auto://localhost:5671"/>
```
### Enabling AUTO over SSL

To configure ActiveMQ auto wire format detection over an SSL connection use the `auto+ssl` transport prefix. For example, add the following transport configuration in your XML file:
```
<transportConnector name="auto+ssl" uri="auto+ssl://localhost:5671"/>
```
*   For more details on using SSL with ActiveMQ, see the following article ([How do I use SSL](how-do-i-use-ssl)).

### Enabling AUTO over NIO

To configure ActiveMQ auto wire format detection over an NIO TCP connection use the `auto+nio` transport prefix. For example, add the following transport configuration in your XML file:
```
<transportConnector name="auto+nio" uri="auto+nio://localhost:5671"/>
```
### Enabling AUTO over NIO SSL

To configure ActiveMQ auto wire format detection over an NIO SSL connection use the `auto+nio+ssl` transport prefix. For example, add the following transport configuration in your XML file:
```
<transportConnector name="auto+nio+ssl" uri="auto+nio+ssl://localhost:5671"/>
```

### Configuring AUTO Transport Options

There are some configuration options that can be set.

Parameter Name|Default Value|Description
---|---|---
protocolDetectionTimeOut|30000|The time before a connection times out in milliseconds. This is similar to maxInactivityDuration. If a client makes a connection to but doesn't send data or enough data for the protocol to be detected then the thread will sit and wait for more data to come in over the socket. This will let the broker kill the connections if they do not complete the protocol initialization after a certain period of time. The default is 30 seconds. Set a default to <= 0 to disable this.
maxConnectionThreadPoolSize|MAX_INT|This option allows the configuration of the maximum size of the thread pool that handles connection attempts. Lowering this number can help prevent the broker from running out of threads if there are many different clients attempting to connect at the same time. By default it is turned off by setting to MAX_INT
  
An example that configures the transport with a maximum protocol detection time of 5 seconds:
```
<transportConnector name="auto" uri="auto://localhost:5671?protocolDetectionTimeOut=5000"/>
```

### Configuring Wire Formats

OpenWire is the default Wire Format that ActiveMQ uses.  It provides a highly efficent binary format for high speed messaging.  OpenWire options can be configured on a JMS client's connection URI string or on a Brokers transport bind URI.

Parameter Prefix|Description
---|---
wireFormat.|Applies the option to all wire formats.
wireFormat.default.|Applies the option to the default format which is OpenWire
wireFormat.stomp.|Applies the option to the STOMP wire format
wireFormat.amqp.|Applies the option to the AMQP wire format
wireFormat.mqtt.|Applies the option to the MQTT wire format

An example of a property that applies to all formats:
```
<transportConnector name="auto" uri="auto://localhost:5671?wireFormat.maxFrameSize=1000"/>
```
An example of a property only applied to OpenWire would be:
```
<transportConnector name="auto" uri="auto://localhost:5671?wireFormat.default.maxFrameSize=1000"/>
```

### Configuring Enabled Wire Protocols

By default all wire protocols are available.  This can be configured to only enable certain formats by setting the property  auto`.protocols.` 

Value|Description
---|---
default|Enables OpenWire
amqp|Enables AMQP format
stomp|Enables STOMP format
mqtt|Enables MQTT format

An example showing only OpenWire and STOMP enabled:
```
<transportConnector name="auto" uri="auto://localhost:5671?auto.protocols=default,stomp"/>
```
