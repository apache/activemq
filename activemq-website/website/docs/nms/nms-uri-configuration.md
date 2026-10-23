---
title: "NMS URI Configuration"
sidebar_position: 3
---

When working with the generic NMS API NMSConnectionFactory class you must choose via the URI which NMS provider you actually wish to use. This page documents the uri syntax and available options for the NMS providers.

### Connection URI Parameters

#####  Example Configuration
```
cf = new NMSConnectionFactory("activemq:tcp://localhost:61616");
```
### NMS Provider Options

Click on the provider name to visit the URI configuration documentation for your chosen NMS provider.

|Option Name|Description|
|---|---|
|[amqp](/components/nms/providers/amqp/uri-configuration)|Use the Apache.NMS.AMQP Provider to talk to ActiveMQ 5.x, ActiveMQ Artemis or any other AMQP 1.0 compliant Broker.|
|[activemq (openwire)](/components/nms/providers/activemq/uri-configuration)|Use the NMS.ActiveMQ Provider to talk to an ActiveMQ Broker that supports openwire.|
