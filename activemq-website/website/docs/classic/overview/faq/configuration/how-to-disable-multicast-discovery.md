---
title: "How to disable multicast discovery"
slug: /how-to-disable-multicast-discovery
---

By default, the [ActiveMQ xml configuration](xml-configuration) includes the [multicast discovery](discovery) mechanism. The tcp transport connector advertises its self using multicast and a multicast network connector is configured to listen to the same address. In this way, all brokers that share the default multicast address will automatically network with each other.  
If multicast is not required, the multicast attributes of the Transport Connector and Network Connector can be removed.

To stop advertising your connection URI on the multicast network remove the discoveryUri attribute from the `<transportConnector/>`:

replace:
```
<transportConnector name="openwire" uri="tcp://localhost:61616" discoveryUri="multicast://default"/>
```
with:
```
<transportConnector name="openwire" uri="tcp://localhost:61616" />
```
If you do not require any networked broker support remove the <networkConnector/> altogether. Remove
```
<networkConnector name="default-nc" uri="multicast://default"/>
```
Alternatively, provide a static networkConnector for each broker you wish to network with by replacing the the discoveryUri with the static transport connection URI of your target broker.

For more information see the [Discovery Transport Reference](discovery-transport-reference)

