---
title: "Peer Transport Reference"
description: "Build a peer-to-peer network of embedded ActiveMQ Classic brokers with the peer:// transport, using the peer://peer-group/brokerName?brokerOptions syntax."
slug: /peer-transport-reference
---


### The Peer Transport

The Peer transport provides a peer-to-peer network with ActiveMQ. What actually happens is the peer transport uses the VM transport to create and connect to a local embedded broker but which configures the embedded broker to establish network connections to other peer embedded brokers.

#### Configuration Syntax
```
peer://peer-group/brokerName?brokerOptions
```

##### Example URI
```
peer://groupa/broker1?persistent=false
```
