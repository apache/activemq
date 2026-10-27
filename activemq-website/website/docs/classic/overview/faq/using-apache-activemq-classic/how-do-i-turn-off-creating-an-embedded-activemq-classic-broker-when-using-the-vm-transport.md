---
title: "How do I turn off creating an embedded ActiveMQ broker when using the VM transport"
slug: /how-do-i-turn-off-creating-an-embedded-activemq-classic-broker-when-using-the-vm-transport
---

You can turn off auto creation by setting the create property on the VM Transport to false:
```
ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?create=false");
```