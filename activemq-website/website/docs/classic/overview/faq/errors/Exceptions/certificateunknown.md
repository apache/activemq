---
title: "certificate_unknown"
slug: /certificateunknown
---

If you get an error something like this...
```
javax.jms.JMSException: start failed: Received fatal alert: certificate_unknown
       at org.activemq.transport.tcp.TcpTransportChannel.start(TcpTransportChannel.java:200)
       at org.activemq.broker.impl.BrokerConnectorImpl.addClient(BrokerConnectorImpl.java:308)
       at org.activemq.transport.TransportServerChannelSupport.addClient(TransportServerChannelSupp
       at org.activemq.transport.tcp.TcpTransportServerChannel.run(TcpTransportServerChannel.java:1
       at java.lang.Thread.run(Unknown Source)
Caused by: javax.net.ssl.SSLHandshakeException: Received fatal alert: certificate_unknown
       at com.sun.net.ssl.internal.ssl.Alerts.getSSLException(Unknown Source)
       at com.sun.net.ssl.internal.ssl.Alerts.getSSLException(Unknown Source)
       at com.sun.net.ssl.internal.ssl.SSLSocketImpl.recvAlert(Unknown Source)
       at com.sun.net.ssl.internal.ssl.SSLSocketImpl.readRecord(Unknown Source)
       at com.sun.net.ssl.internal.ssl.SSLSocketImpl.performInitialHandshake(Unknown Source)
       at com.sun.net.ssl.internal.ssl.SSLSocketImpl.readDataRecord(Unknown Source)
       at com.sun.net.ssl.internal.ssl.AppInputStream.read(Unknown Source)
       at java.io.BufferedInputStream.fill(Unknown Source) 
```
when you are trying to use SSL to connect to ActiveMQ then the "certificate_unknown" error shows on the broker when the client doesn't trust the broker's certificate. On the client, I would see an error as well: "No trusted certificate found".

### Fix

Make sure that you exported the broker's certificate (step 2 in [How do I use SSL](how-do-i-use-ssl)) and imported it on the client into a truststore (step 4). If you did those, did you specify the javax.net.ssl.trustStore system property when you started your client VM?

