---
title: "Apache.NMS.AMQP Configuration"
---

# Client configuration
This file details various configuration options for the client, the syntax for its related configuration, and various uri options that can be set when defining a ConnectionFactory. 

## Connection Uri
The basic format of the clients Connection URI is as follows:

```
amqp[s]://hostname:port[?option=value[&option2=value...]]
```

Where the amqps and scheme is specified to use SSL/TLS.

The client can be configured with a number of different settings using the uri while defining the ConnectionFactory, these are detailed in the following sections.

### NMS Configuration options
The options apply to the behavior of the NMS objects such as Connection, Session, MessageConsumer and MessageProducer.

- **nms.username** User name value used to authenticate the connection.
- **nms.password** The password value used to authenticate the connection.
- **nms.clientId** The ClientId value that is applied to the connection.
- **nms.localMessageExpiry** Controls whether MessageConsumer instances will locally filter expired Messages or deliver them. By default this value is set to true and expired messages will be filtered.
- **nms.sendTimeout** Timeout value that controls how long the client waits on completion of a synchronous message send before returning an error. By default the client will wait indefinitely for a send to complete.
- **nms.requestTimeout** Timeout value that controls how long the client waits on completion of various synchronous interactions, such as opening a producer or consumer, before returning an error. Does not affect synchronous message sends. By default the client will wait indefinitely for a request to complete.
- **nms.clientIdPrefix** Optional prefix value that is used for generated Client ID values when a new Connection is created for the JMS ConnectionFactory. The default prefix is 'ID:'.
- **nms.connectionIdPrefix** Optional prefix value that is used for generated Connection ID values when a new Connection is created for the JMS ConnectionFactory. This connection ID is used when logging some information from the JMS Connection object so a configurable prefix can make breadcrumbing the logs easier. The default prefix is 'ID:'.

### TCP Transport Configuration options
When connected to a remote using plain TCP these options configure the behaviour of the underlying socket. These options are appended to the connection URI along with the other configuration options, for example:

```
amqp://localhost:5672?nms.clientId=foo&transport.receiveBufferSize=30000
```

The complete set of TCP Transport options is listed below:

- **transport.sendBufferSize** Specifies the ReceiveBufferSize option of the TCP socket.
- **transport.receiveBufferSize** Specifies the SendBufferSize option of the TCP socket.
- **transport.receiveTimeout** Specifies the ReceiveTimeout option of the TCP socket.
- **transport.sendTimeout** Specifies the SendTimeout option of the TCP socket.
- **transport.tcpKeepAliveTime** Specifies how often a keep-alive transmission is sent to an idle connection.
- **transport.tcpKeepAliveInterval** Specifies how often a keep-alive transmission is sent when no response is received from previous keep-alive transmissions.
- **transport.tcpNoDelay** Specifies the NoDelay option of the TCP socket.

If *tcpKeepAliveTime* or *tcpKeepAliveInterval* it set, TCP Keep-Alive is enabled.

### Failover Configuration options
With failover enabled the client can reconnect to another server automatically when connection to the current server is lost for some reason. The failover URI is always initiated with the failover prefix and a list of URIs for the server(s) is contained inside a set of parentheses. The "nms." options are applied to the overall failover URI, outside the parentheses, and affect the NMS Connection object for its lifetime.

The URI for failover looks something like the following:

```
failover:(amqp://host1:5672,amqp://host2:5672)?nms.clientId=foo&failover.maxReconnectAttempts=20
```

The individual broker details within the parentheses can use the "transport." or "amqp." options defined earlier, with these being applied as each host is connected to:

```
failover:(amqp://host1:5672?amqp.option=value,amqp://host2:5672?transport.option=value)?nms.clientId=foo
```

The complete set of configuration options for failover is listed below:

- **failover.initialReconnectDelay** The amount of time the client will wait before the first attempt to reconnect to a remote peer. The default value is zero, meaning the first attempt happens immediately.
- **failover.reconnectDelay** Controls the delay between successive reconnection attempts, defaults to 10 milliseconds. If the backoff option is not enabled this value remains constant.
- **failover.maxReconnectDelay** The maximum time that the client will wait before attempting a reconnect. This value is only used when the backoff feature is enabled to ensure that the delay doesn't not grow too large. Defaults to 30 seconds as the max time between connect attempts.
- **failover.useReconnectBackOff** Controls whether the time between reconnection attempts should grow based on a configured multiplier. This option defaults to true.
- **failover.reconnectBackOffMultiplier** The multiplier used to grow the reconnection delay value, defaults to 2.0d.
- **failover.maxReconnectAttempts** The number of reconnection attempts allowed before reporting the connection as failed to the client. The default is no limit or (-1).
- **failover.startupMaxReconnectAttempts** For a client that has never connected to a remote peer before this option control how many attempts are made to connect before reporting the connection as failed. The default is to use the value of maxReconnectAttempts.
