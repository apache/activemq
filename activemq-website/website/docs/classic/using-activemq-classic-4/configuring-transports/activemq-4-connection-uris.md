---
title: "ActiveMQ 4 Connection URIs"
slug: /activemq-4-connection-uris
---


Transport configuration options
-------------------------------

One of the first kinds of URI you are likely to use is a transport URI to connect to a broker using a kind of transport. Generally TCP or VM are the first transports you'll use.

Be careful about whitespace

All of the following URI configurations are based on the java.net.URI class which does not allow whitespace to be used. So if you are using **failover:** or **static:** URIs, do not put any whitespace around the **,** symbol.

### The AUTO Transport

Starting with 5.13.0 ActiveMQ has support for automatic wire protocol detection over TCP, SSL, NIO, and NIO SSL.  OpenWire, STOMP, AMQP, and MQTT are supported.  For details see the [AUTO](auto) Transport Reference.

### The VM Transport

The VM transport allows clients to connect to each other inside the VM without the overhead of the network communication. The connection used is not that of a socket connection but instead uses direct method invocations to enable a high performance embedded messaging system.

The first client to use the VM connection will boot an embedded broker. Subsequent connections will attach that the same broker. Once all VM connections to the broker have been closed, the embedded broker will automatically shutdown.

For more information see the [VM Transport Reference](vm-transport-reference)

### The AMQP Transport

As of 5.8.0 ActiveMQ has support for AMQP. For details see the [AMQP](amqp) Transport Reference.

### The MQTT Transport

Starting with 5.6.0 ActiveMQ also supports [MQTT](http://mqtt.org/). Its a light weight publish/subscribe messaging transport. See the [MQTT](mqtt) Transport Reference for details.

### The TCP Transport

The TCP transport allows clients to connect a remote ActiveMQ using a a TCP socket.

For more information see the [TCP Transport Reference](tcp-transport-reference)

### The NIO Transport

Same as the TCP transport, except that the [New I/O (NIO)](http://en.wikipedia.org/wiki/New_I/O) package is used, which may provide better performance. The Java NIO package should not be confused with IBM's [AIO4J](http://java.sys-con.com/read/46658.htm) package.

To switch from TCP to NIO, simply change the scheme portion of the URI. Here's an example as defined within a broker's XML configuration file.
```
<broker>
  ...
  <transportConnectors>
    <transportConnector name="nio" uri="nio://0.0.0.0:61616"/>  
  </<transportConnectors>
  ...
</broker>
```
Trying to use nio transport url on the client side will instantiate the regular TCP transport.

For more information see the [NIO Transport Reference](nio-transport-reference)

### The SSL Transport

This allows you to talk over TCP using SSL.

For more information see the [SSL Transport Reference](ssl-transport-reference)

### The NIO SSL Transport

> **Availability**
> 
> Available since 5.6

Implementing SSL transport over NIO. This allows you to connect large number of SSL clients to a single broker instance. It's server side transport-option only
```
<broker>
  ...
  <transportConnectors>
    <transportConnector name="nio+ssl" uri="nio+ssl://0.0.0.0:61616"/>  
  </<transportConnectors>
  ...
</broker>
```
Trying to use `nio+ssl` transport url on the client side will instantiate the regular SSL transport.

### The Peer Transport

The Peer transport provides a peer-to-peer network with ActiveMQ. What actually happens is the peer transport uses the VM transport to create and connect to a local embedded broker but which configures the embedded broker to establish network connections to other peer embedded brokers.

For more information see the [Peer Transport Reference](peer-transport-reference)

### The UDP Transport

This allows you to talk over UDP.

For more information see the [UDP Transport Reference](udp-transport-reference)

### The Multicast Transport

This allows you to talk over Multicast.

For more information see the [Multicast Transport Reference](multicast-transport-reference)

### The HTTP and HTTPS Transport

This allows the ActiveMQ client and broker to tunnel over HTTP. If the client is not JMS you might want to look at [REST](rest) or [Ajax](ajax) support instead.

For more information see the [HTTP and HTTPs Transports Reference](http-and-https-transports-reference)

### The WebSockets Transport

This transport uses the new HTML5 WebSockets to exchange messages with the broker. For more information see the [WebSockets](websockets) Transport Reference

### The Stomp Transport

A plain text transport that can be used with many languages. See [Stomp](stomp) for more details.

General Purpose URIs
--------------------

You can configure other features via the URI syntax as follows...

### Connection Configuration URI

Any Apache ActiveMQ JMS connection can be configured using the URL or explicitly setting properties on the [ActiveMQConnection](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQConnection.html) or [ActiveMQConnectionFactory](http://incubator.apache.org/activemq/maven/activemq-core/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html) objects themselves.

For more information see [Connection Configuration URI](connection-configuration-uri)

### Destination Options

You can configure various consumer related options using [Destination Options](destination-options) which allow you to configure destinations using URI syntax.

### Broker Configuration URI

You can use a [Broker Configuration URI](broker-configuration-uri) to configure an embedded broker, either using the BrokerFactory helper class from Java or using the activemq shell script. For more details see [How to Run a Broker](run-broker)

### Configuring Wire Formats

Any transport which involves marshalling messages onto some kind of network transport like TCP or UDP will typically use the [OpenWire](openwire) format. This is configurable to customize how things appear on the wire.

For more information see [Configuring Wire Formats](configuring-wire-formats)

High Level Protocol URIs
------------------------

The following higher level protocols can be configured via URI

### The Failover Transport

The Failover transport layers reconnect logic on top of any of the other transports. This is what used to be the Reliable transport in ActiveMQ 3. Its configuration syntax allows you to specify any number of composite URIs. The Failover transport randomly chooses one of the composite URIs and attempts to establish a connection to it. If it does not succeed or if it subsequently fails, a new connection is established to one of the other URIs in the list.

For more information see the [Failover Transport Reference](failover-transport-reference)

### The Fanout Transport

The Fanout transport layers reconnect and replicate logic on top of any of the other transports. It is used replicate commands to multiple brokers.

For more information see the [Fanout Transport Reference](fanout-transport-reference)

Using Discovery
---------------

Often when using transports like TCP you want to use [Discovery](discovery) to locate the available brokers. This is different from using, say, [Multicast](multicast-transport-reference) \- as the actual main communication is over TCP but multicast is purely used to discover the location of brokers.

### The Discovery Transport

The Discovery transport works just like the reliable transport, except that it uses a discovery agent to locate the list of URIs to connect to.

For more information see the [Discovery Transport Reference](discovery-transport-reference)

### The ZeroConf Transport

The ZeroConf transport provides [Discovery](discovery) and it works like the [Discovery Transport](discovery-transport-reference) but rather than using our own multicast based discovery mechanism (which allows you to configure the exact multicast address and port, etc.), the [ZeroConf](zeroconf) transport is used instead.

For more information see the [ZeroConf Transport Reference](zeroconf-transport-reference)

### Server side options

There are a number of options that can be used for changing behaviour on the server for the TransportConnector in the ActiveMQ broker configuration. These are:
property name|default|description
uri|null|the bind address for the transport
name|null|the name of the TransportConnector instance
discoveryURI|null|if set, the multicast discovery address for client connections to find the broker
enableStatusMonitor|false|will monitor connections to determine if they are blocked
updateClusterClients|false|if enabled, will update client connections (if they use the failover:// transport) of changes to the broker cluster
rebalanceClusterClients|false|will automatically rebalance clients across the cluster on changes of topology
updateClusterClientsOnRemove|false|will update clients if a broker is removed from the cluster
updateClusterFilter|null|comma separated list of regular expressions. Brokers with the a name matching the pattern will be included for client updates
allowLinkStealing|false|this is enabled for default for mqtt transport. Link Stealing is where the last of two or more connections with the same id (clinetId for JMS) is deemed the valid connection and the older one is closed by the broker.

An example configuration would be:
```
<broker>
  ...
  <transportConnectors>
    <transportConnector name="openwire" uri="tcp://0.0.0.0:61616" enableStatusMonitor="true"/>  
  </<transportConnectors>
  ...
</broker>
```