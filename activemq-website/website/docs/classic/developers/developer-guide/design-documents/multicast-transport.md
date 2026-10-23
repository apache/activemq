---
title: "Multicast Transport"
slug: /multicast-transport
---


We could support a reliable multicast protocol and use that to distribute messages across. This page braindumps how that could fit in with ActiveMQ 4.x's architecture.

*   each VM would have its own embedded broker; JMS connections would talk VM protocol to the embedded broker (or TCP to some remote broker)
*   the brokers communicate with each other via multicast
    *   the exception to this could be queue dispatch/ack messages could go over UDP rather than mcast.

So the brokers communicate with a kind of Bridge which uses the multicast/UDP transports underneath.

### Broker keep-alive

Each broker will repeatedly send its own BrokerInfo to the cloud. The MulticastBridge will receive them and keep that broker alive; brokers which don't send the brokerInfo within the heartbeat window are logically considered to have closed.

