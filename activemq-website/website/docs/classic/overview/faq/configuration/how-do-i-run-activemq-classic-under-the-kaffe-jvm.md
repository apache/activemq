---
title: "How do I run ActiveMQ under the Kaffe JVM"
slug: /how-do-i-run-activemq-classic-under-the-kaffe-jvm
---

ActiveMQ will run under Kaffe with a few adjustments to the default configuration.

We have found the Kaffe does not properly implement:

*   File based NIO
*   Multicast Sockets
*   JMX connector JNDI handling

Therefore, the default ActiveMQ configuration must be adjusted to disable the Journal (uses NIO), disable Multicast discovery, and JMX.

The Kaffe and OS Version that was tested against was:

> kaffe version info: Engine: Interpreter Version: 1.1.7 Java Version: 1.4  
> OS & version: Linux dev-10 2.6.13-15.8-smp #1 SMP Tue Feb 7 11:07:24 UTC

The following is the activemq.xml that was used:
```
<beans>
 <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

 <broker useJmx="false" xmlns="http://activemq.org/config/1.0">

  <persistenceAdapter>
     <journaledJDBC useJournal="false" dataDirectory="activemq-data"/>
  </persistenceAdapter>

  <transportConnectors>
     <transportConnector name="default" uri="tcp://localhost:61616"/>
     <transportConnector name="stomp"   uri="stomp://localhost:61613"/>
  </transportConnectors>

 </broker>

</beans>
```