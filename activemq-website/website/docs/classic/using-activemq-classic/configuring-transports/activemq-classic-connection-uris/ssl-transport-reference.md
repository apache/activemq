---
title: "SSL Transport Reference"
description: "Connect to ActiveMQ Classic over ssl://, set SSLServerSocket options via transport. and socket., configure ActiveMQSslConnectionFactory and hostname validation."
slug: /ssl-transport-reference
---


### The SSL Transport

The SSL transport allows clients to connect to a remote ActiveMQ broker using SSL over a TCP socket.

#### Configuration Syntax

**ssl://hostname:port?transportOptions**

#### Transport Options

The configuration options from [TCP](tcp-transport-reference) are relevant.

#### Example URI
```
ssl://localhost:61616?trace=false
```
#### SSLServerSocket options

From version 5.4 any [SSLServerSocket](http://java.sun.com/j2se/1.4.2/docs/api/javax/net/ssl/SSLServerSocket.html) option may be set on a TransportConnection via **?transport.XXX**, for example:
```
ssl://localhost:61616?transport.enabledCipherSuites=SSL_RSA_WITH_RC4_128_SHA,SSL_DH_anon_WITH_3DES_EDE_CBC_SHA
ssl://localhost:61616?transport.needClientAuth=true
```
#### Client configuration

JMS clients can simply use the [ActiveMQSslConnectionFactory](http://activemq.apache.org/maven/5.9.0/apidocs/org/apache/activemq/ActiveMQSslConnectionFactory.html) together with an `ssl://` broker url as the following Spring configuration illustrates

```xml
<bean id="AMQJMSConnectionFactory" class="org.apache.activemq.ActiveMQSslConnectionFactory">
  <property name="trustStore" value="/path/to/truststore.ts" />
  <property name="trustStorePassword" value="password" />
  <property name="keyStore" value="/path/to/keystore.ks" />
  <property name="keyStorePassword" value="password" />
  <property name="brokerURL" value="ssl://localhost:61616" />
  <property name="userName" value="admin" /> 
  <property name="password" value="admin" />
</bean> 
```
Unless the broker's SSL transport is configured for transport.needClientAuth=true, the client won't need a keystore but requires a truststore in order to validate the broker's certificate.

Similar to the broker transport configuration you can pass on SSL transport options using **?socket.XXX**, such as
```
ssl://localhost:61616?socket.enabledCipherSuites=SSL_RSA_WITH_RC4_128_SHA,SSL_DH_anon_WITH_3DES_EDE_CBC_SHA
```
#### Hostname Validation (Starting with version 5.15.6)

From version 5.15.6 ActiveMQ now supports TLS Hostname validation. This has been enabled by default for the ActiveMQ client and is off by default on the broker. To configure:

#### Server side configuration of hostname validation

The default for the server side is to disable Hostname validation and this can be configured with ?transport.verifyHostName.  This is only relevant for 2-way SSL and will cause the client's CN of their certificate to be compared to their hostname to verify they match, e.g.:
```
ssl://localhost:61616?transport.verifyHostName=true
```
#### Client side configuration of hostname validation

The default for the ActiveMQ client is to enable Hostname validation and this can be configured with `?socket.verifyHostName` or simply `?verifyHostName` with no prefix. This will cause the CN of the server certificate to be compared to the server hostname to verify they match, e.g.:
```
ssl://localhost:61616?socket.verifyHostName=false
```
or:
```
ssl://localhost:61616?verifyHostName=false
```
#### Other Links

* [How do I use SSL](how-do-i-use-ssl)

You can also turn on SSL debug informations this way by adding:
```
-Djavax.net.debug=ssl
```
this way you can see what goes wrong and why you get connections closed.

#### Be careful with multicast discovery

If your XML configuration file contains the following and you wish to use SSL
```xml
<networkConnector uri="multicast://default"/>
```
Then you will currently need to comment that out. The reason is to prevent ActiveMQ atempting to connect to itself - if you do this with a self-signed certificate, you will get a constant spam of certificate_unknown stacktraces to the console, as the broker is not configured with the truststore.
