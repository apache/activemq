---
title: "Discovery Transport Reference"
description: "Use the ActiveMQ Classic discovery: transport to find brokers through a discovery agent such as multicast://default, with reconnect and backoff options."
slug: /discovery-transport-reference
---


### The Discovery Transport

The Discovery transport works just like the [Failover](failover-transport-reference) transport, except that it uses a discovery agent to locate the list of uri to connect to. The Discovery transport is also used by the [Fanout](fanout-transport-reference) transport for discovering brokers to send a fanout message to.

#### Configuration Syntax

```
discovery:(discoveryAgentURI)?transportOptions
```
or  
```
discovery:discoveryAgentURI
```

Note that to be able to use [Discovery](discovery) to find brokers, the brokers need to have the multicast discovery agent enabled on the broker.

To configure discovery in a Broker you should use the [Xml Configuration](xml-configuration). Here is an [example](http://svn.apache.org/viewvc/activemq/trunk/activemq-core/src/test/resources/org/apache/activemq/usecases/receiver-discovery.xml?view=co) of using discovery. Its basically something like the following (see the **discoveryUri**)
```
<broker name="foo">
  <transportConnectors>
    <transportConnector uri="tcp://localhost:0" discoveryUri="multicast://default"/>
  </transportConnectors>

  ...
</broker>
```

##### Transport Options

Option Name|Default Value|Description
---|---|---
reconnectDelay|10|How long to wait for discovery
initialReconnectDelay|10|How long to wait before the first reconnect attempt to a discovered url
maxReconnectDelay|30000|The maximum amount of time we ever wait between reconnect attempts
useExponentialBackOff|true|Should an exponential backoff be used btween reconnect attempts
backOffMultiplier|2|The exponent used in the exponential backoff attempts
maxReconnectAttempts|0|If not 0, then this is the maximum number of reconnect attempts before an error is sent back to the client
group|default|an identifier for the group to partition multi cast traffic among collaborating peers; the group forms part of the shared identity of a discovery datagram (since 5.2)

##### Example URI
```
discovery:(multicast://default)?initialReconnectDelay=100
```
Applying parameters to discovered transports

From 5.4, transport parameters in the URI will also be applied to discovered transports if they are prefixed with `discovered.`; For example, adding the `discovered.connectionTimeout` parameter to the URI will apply the parameter to every discovered [TCP](tcp-transport-reference) transport, even though this parameter is not a Discovery transport option.

