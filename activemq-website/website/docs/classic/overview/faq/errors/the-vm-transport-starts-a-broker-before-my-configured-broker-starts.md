---
title: "The vm transport starts a broker before my configured broker starts"
slug: /the-vm-transport-starts-a-broker-before-my-configured-broker-starts
---

### Scenario

You are using the **vm:** transport and a broker is auto-started for you so that your configured embedded broker doesn't start.

### Solution

Its most likely a dependency issue; your JMS connection is starting before your embedded broker. So just make sure your embedded broker is started first.

Another cause could be that your use of the [VM Transport](vm-transport-reference) defines a broker name of localhost whereas your embedded broker uses a different name; so just be consistent in the naming.

ActiveMQ 5.2 adds the waitForStart='timeout in milliseconds' option to the VM transport which forces the VM transport to wait till the broker is started. Coupled with the "create=false" option, it is possible to ensure that a single embedded broker is auto-started.

### See also

*   [VM Transport Reference](vm-transport-reference)

