---
title: "The Proxy Connector"
slug: /the-proxy-connector
---


The Proxy Connector
-------------------

Many of the more advanced ActiveMQ features are implemented in the transports. Sometimes it's desirable to accept one kind of ActiveMQ connection and establish a different kind of connection to another broker. The Proxy Connector allows you to do that by accepting connection on one transport and establishing new connections using a different transport and then just proxying all the commands sent between the two transports.

### Example Configuration

If you wanted to accept normal connection on port 6001, but also wanted to expose port 6002 which in turn was proxied using the fanout transport, the following example is shows you how to do it!

```
<beans xmlns="http://activemq.org/config/1.0">
  <broker>

    <persistenceAdapter>
      <journaledJDBC dataDirectory="../data"/>
    </persistenceAdapter>

    <transportConnectors>
      <transportConnector uri="tcp://localhost:6001"/>
    </transportConnectors>

    <proxyConnectors>
      <proxyConnector bind="tcp://localhost:6002" remote="fanout:static:(tcp://localhost:61616)"/>
    </proxyConnectors>

  </broker>
</beans>
```