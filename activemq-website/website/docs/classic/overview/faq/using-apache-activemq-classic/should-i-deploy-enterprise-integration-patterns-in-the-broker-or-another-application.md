---
title: "Should I deploy Enterprise Integration Patterns in the broker or another application"
slug: /should-i-deploy-enterprise-integration-patterns-in-the-broker-or-another-application
---

Should I deploy Enterprise Integration Patterns in the broker or another application
------------------------------------------------------------------------------------

Whether you deploy the [Enterprise Integration Patterns](enterprise-integration-patterns) inside the ActiveMQ Broker or in a separate application depends on your requirements.

### Advantages of deploying EIP inside the broker

*   Its a single JVM so less moving parts and you're less likely to forget to deploy something
*   if you are doing things like polling resources such as files, databases and bridging them to queues or topics then its usually more efficient to host in the broker; as there's less contention and there's no network communication between the EIP rules and the message broker as its all in the same JVM (so you can use the [VM Transport](vm-transport-reference) to avoid network overhead.

### Advantages of deploying EIP inside a separate application

*   its easier to deploy loads of JVMs containing the EIP routing rules to get better load balancing
*   you can easily change your routing rules then stop/restart applications without having to restart your broker. Having said that since ActiveMQ supports [auto reconnection](how-can-i-support-auto-reconnection) its not such a big deal to bounce the Broker now and again to refresh routing rules. Note that at some point [Camel will support auto-reload of routing rules on the fly without having to stop and reload the JVM](https://issues.apache.org/activemq/browse/CAMEL-234) so one day this won't be such a big benefit.

### See Also

*   [Should I deploy the broker inside my JVM or AppServer](should-i-deploy-the-broker-inside-my-jvm-or-appserver)

