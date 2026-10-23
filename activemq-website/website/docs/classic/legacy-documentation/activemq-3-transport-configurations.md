---
title: "ActiveMQ 3 Transport Configurations"
slug: /activemq-3-transport-configurations
---

Some of the transports support configuration, such as timeout values, maximum retry count and so forth. To configure the transports you can use the URI query syntax. e.g.

If you are using a list of URLs like above with the _list_ and _reliable_ protocols you can wrap the list in parens to be able to configure the list. e.g.

### Generic configuration options

Option Name|Default Value|Type
---|---|---
brokerXmlConfig|null|String
cachingEnabled|true|boolean
clientID|null|String
copyMessageOnSend|true|boolean
disableTimeStampsByDefault|false|boolean
doMessageCompression|true|boolean
doMessageFragmentation|true|boolean
messageCompressionLimit|32768|int
messageCompressionStrategy|0|int
messageFragmentationLimit|65536|int
optimizedMessageDispatch|false|boolean
password|null|String
prepareMessageBodyOnSend|true|boolean
turboBoost|true|boolean
useAsyncSend|true|boolean
useEmbeddedBroker|false|boolean
userName|null|String
wireFormat|default|String
noDelay|false|boolean

#### brokerXmlConfig

Sets the [Xml Configuration](xml-configuration) file used to configure the ActiveMQ broker via Spring if using embedded mode. The filename is assumed to be on the classpath unless a URL is specified. So a value of


```
foo/bar.xml
```

would be assumed to be on the classpath whereas

```
file:dir/file.xml
```

would use the file system. Any valid URL string is supported.

#### cachingEnabled

Provides optimisations on the wire format (caching data at each end and using references to it to minmise the message size

#### clientID

Used to set the JMS clientID of connection.

#### copyMessageOnSend

Use to avoid the overhead of doing a deep copy of the JMS message everytime the message is sent.

#### disableTimeStampsByDefault

Use to avoid the overhead of setting a timestamp on each message produced.

#### doMessageCompression

If messages should be compressed.

#### doMessageFragmentation

If messages should be chunked into smaller fragments on the clients side.

#### messageCompressionLimit

Data size above which compression will be used

#### messageCompressionStrategy

The compression strategy to use when compressing messages. Defaults to the value of java.util.zip.Deflater.DEFAULT_STRATEGY (0).

#### messageFragmentationLimit

Data size above which message fragmentation will be used

#### optimizedMessageDispatch

#### password

The user password used to authenticate with the broker.

#### prepareMessageBodyOnSend

Causes pre-serialization of messages before send.

#### turboBoost

Enables a number of performance enhancements which are disabled by default to ensure J2EE certification. turboBoost turns on a bunch of optimisations: disableTimeStampsByDefault, useAsyncSend, cachingEnabled, and optimizedMessageDispatch and then disables copyMessageOnSend and prepareMessageBodyOnSend

#### useAsyncSend

Used to enable asynchronous sending, which boost performance by using another thread to handle sending of messages to the socket.

#### useEmbeddedBroker

Set to true if you want start an embeded broker when the first connection is created.

#### userName

The user name used to authenticate with the broker.

#### wireFormat

Allows a custom wire format to be used; otherwise the default Java wire format is used which is designed for minimum size and maximum speed on the Java platform.

#### noDelay

Used to indicate whether messages are dispatched as quickly as possible or not. (The latency versus throughput trade off). When using TCP based transport this option is a means of disabling Nagles which can often improve performance on synchronous sends.

### TCP configuration options

*   soTimeout (long) sets the [SO_TIMEOUT](http://java.sun.com/j2se/1.4.2/docs/api/java/net/Socket.html#setSoTimeout(int)) value (in milliseconds) on the socket
*   socketBufferSize (int) the size of the buffer size on the socket

### SSL configuration options

Same as TCP. SSL certificates are configured at the VM level. See [How do I use SSL](how-do-i-use-ssl)

### List configuration options

*   maximumRetries (int) the maximum number of times that this protocol will attempt to make a connection on a failure before giving up completely and throwing an exception up to the JMS client. Set this value to zero (or -1) to force an infinite reconnection loop. **Warning**: setting an infinite reconnection loop could hang your JMS client program indefinitely.
*   failureSleepTime (long) the amout of time that the transport will sleep before trying to reconnect again
*   establishConnectionTimeout (long) the timeout value for which a connection must start by on connection start - otherwise the start connection will fail
*   incrementTimeout (boolean) should the timeout increase, exponentially as connection attempts are made
*   maximumTimeout (long) the maximum timeout value that it should increase to (if enabled)

### Reliable and Remote configuration options

Same as the List configuration options above - plus

*   keepAliveTimeout (long) the amount of time without receiving a keep alive ping before the connection is considered dead.

### Peer configuration options

*   discoveryURI (string) the URI used to discover find other nodes; which defaults to using multicast discovery
*   remoteUserName, removePassword (string) the optional login/password to connect to the remote brokers with
*   peerURIs (string) the hard coded URIs of peers to connect to

### Discovery configuration options

*   channelName (string) provides a way to separate out different logical groups of nodes in the cluster

### Multicast configuration options

*   timeToLive (int) defaults to 1 so we don't send multicast messages beyond our own local subnet.
*   loopbackMode (boolean) defaults to false so we don't see our own messages
