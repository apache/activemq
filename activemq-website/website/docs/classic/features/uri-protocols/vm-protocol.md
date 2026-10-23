---
title: "VM Protocol"
slug: /vm-protocol
---


Protocol syntax : vm://logicalname

Javadocs : [org.codehaus.activemq.transport.vm](http://activemq.codehaus.org/maven/apidocs/org/codehaus/activemq/transport/vm/package-frame.html)

The VM protocol allows clients to connect to each other inside the VM without the overhead of the network communication. The connection used is not a socket connection but some kind of virtual connection that allows an embedded messaging system.

It is to be noted that objects inside JMS ObjectMessage instances are transferred using serialization and not by reference. This may sound surprising but there are very good reasons for that.

1.  In a well designed J2EE application server (from a production point of view), you typically have a classloader hierarchy that allows to run EARs and WARs in perfect isolation between each other and from the third-party libraries used by the server internals. If you have clients located in different, isolated classloader hierarchies then a shared reference will lead to a ClassCastException. Thus it **must** be serialized..
2.  If you were to send an object by reference, it would be wrapped by an [ObjectMessage](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/ObjectMessage.html). Nothing would then prevent you from modifying the object further before sending it. That may sound like it is not a problem but the TCK tests exactly for that, meaning that your JMS implementation would not be J2EE-compliant. Therefore the object **must** be serialized.

If intra-vm JMS performance were an issue when sending messages in your application, and that you know your messaging boundaries are constrained in your classloader scope, then you could perfectly think of superfast vm protocol that would send messages by reference. This currently does not exist within ActiveMQ for the reasons stated above.

