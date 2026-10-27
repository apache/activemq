---
title: "MQTT"
slug: /mqtt
---


ActiveMQ supports the [MQTT](http://mqtt.org/) protocol and will automatically map between JMS/NMS and MQTT clients. MQTT is a machine-to-machine (M2M) publish/subscribe messaging transport.

Please see the [MQTT site](http://mqtt.org/) for more details

### Supported versions

ActiveMQ supports MQTT [v3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) and [v3.1](https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html).

### Enabling the ActiveMQ Broker for MQTT

Its very easy to enable ActiveMQ for MQTT. Just add a connector to the broker using the MQTT URL.
```
<transportConnectors>
   <transportConnector name="mqtt" uri="mqtt://localhost:1883"/>
</transportConnectors>
```
### The MQTT Wire Format

MQTT uses a compact binary format that can be configured with the following options.  All options can be configured on a Brokers transport bind URI.

Parameter Name|Default Value|Description
---|---|---
maxFrameSize|268435456|(v5.12.0) Maximum frame size that can be sent. The protocol limit is 256 megabytes and his value cannot be set higher. Can help help prevent OOM DOS attacks

All options must be prepended with `wireFormat` in order to take effect. Without this proper formatting, the option will have zero effect.

#### Example Wire Format Configuration  
```
<transportConnector name="mqtt" uri="mqtt://localhost:61612?wireFormat.maxFrameSize=100000"/>
```

### Security

The ActiveMQ MQTT Transport implementation fully supports an [ActiveMQ security](security) mechanism. Also, the authorization policies will be applied when you try to access (read/write) certain destinations.

### Enabling MQTT over NIO

For better scalability (and performance) you might want to run the MQTT protocol over NIO transport. To do that just use `mqtt+nio` transport prefix instead of `mqtt`. For example, add the following transport configuration in your XML file
```
<transportConnector name="mqtt+nio" uri="mqtt+nio://localhost:1883"/>
```

This transport use [NIO transport](configuring-transports) underneath and will generally use much less threads than standard connector.

### Enabling MQTT over NIO + SSL

The MQTT transport also supports using NIO and SSL. To enable this option, use the mqtt+nio+ssl protocol - e.g.
```
<transportConnector name="mqtt+nio" uri="mqtt+nio+ssl://localhost:1883"/>
```

*   For more details on using SSL with ActiveMQ see the following article ([How do I use SSL](how-do-i-use-ssl)).

### Working with Destinations with MQTT

MQTT supports hierarchies and wildcards, though the delimiters and characters are different: - Here's the mapping:

function|ActiveMQ|MQTT
---|---|---
separator|`.`|`/`
element|`*`|`+`
sub tree|`>`|`#`

These values are automatically transposed between clients using JMS/NMS/Stomp and clients using MQTTT. For example - a client subscribing to "foo/#/bar" would receive messages published on a JMS Topic of foo.blah.bar.

### Message transformations

MQTT messages are transformed into an JMS ByteMessage. Conversely, the body of any JMS Message is converted to a byte buffer to be the payload of an MQTT message.

### Keep Alive

When a client connects, it will send a keep-alive duration, usually defaulting to 10s. ActiveMQ will honor the keep-alive duration by setting up an Inactivity Monitor that allows a grace period of 1.5 * duration. After that grace period duration elapses a connection could be closed if there is no activity. A broker receiving a PINGREQ and sending PINGRESP is considered activity to keep the connection opened.

If a client sends a keep-alive value of 0, ActiveMQ will not set up an [Inactivity Monitor](activemq-classic-inactivitymonitor) and connections will not be auto-shutdown due to inactivity. This however can lead to potentially leaky connections, so a default keep alive can be set on the server side (by an admin, for example) to not allow inactive connections to hang. This default keep alive would only be used if specified and if the client requests a keep-alive value of 0. The unit for the keep-alive value is milliseconds.

To enable a default, server-side MQTT keep alive:
```
<transportConnector name="mqtt" uri="mqtt://localhost:1883?transport.defaultKeepAlive=60000"/>
```

### Message Prefetch

When MQTT client connects, it locally create JMS-like consumer to the broker. In older versions this consumer was created with the prefetch size of 1 (message prefetching is explained [here](what-is-the-prefetch-limit-for) in more details). Starting with 5.11.0 release, the prefetch size is adjusted to the default value for the appropriate JMS subscription. QoS=0 subscriptions have default prefetch of regular non-persistent topic subscriptions, while QoS=1 and QoS=2 are assigned prefetch size of durable subscribers or the queue subscriptions depending on the subscription strategy used (see the next section for more details). Default prefetch values are listed [here](what-is-the-prefetch-limit-for).

To change default value of the prefetch size, you can use _activeMQSubscriptionPrefetch_ transport option, like
```
<transportConnector name="mqtt" uri="mqtt://localhost:1883?transport.activeMQSubscriptionPrefetch=32766"/>
```

### Subscription Strategy

ActiveMQ is a JMS broker in its core, so there needs to be some mapping between MQTT subscriptions and JMS semantics. Subscriptions with QoS=0 (At Most Once) are directly mapped to plain JMS non-persistent topics. For reliable messaging, QoS=1 and QoS=2, by default subscriptions are transformed to JMS durable topic subscribers. This behaviour is desired in most scenarios. For some use cases, it is useful to map these subscriptions to [virtual topics](virtual-destinations). Virtual topics provide a better scalability and are generally better solution if you want to use you MQTT subscribers over network of brokers. To change subscription strategy to use virtual topic, use the following settings:
```
<transportConnector name="mqtt" uri="mqtt://localhost:1883?transport.subscriptionStrategy=mqtt-virtual-topic-subscriptions"/>
```

### Retained Messages

If a message has been published with the _retain_ flag set, then the message will be 'remembered' by the topic so that if a new subscription arrives, the last retained message is sent to the subscription. Underneath, the broker uses [retained message subscription recovery policy](subscription-recovery-policy) to retain messages with _ActiveMQ.Retain_ property set. During the message conversion, MQTT messages with retain flag become JMS message with the _ActiveMQ.Retain _property set and retained by the broker.

