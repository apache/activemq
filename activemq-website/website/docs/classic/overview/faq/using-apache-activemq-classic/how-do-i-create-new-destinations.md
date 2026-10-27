---
title: "How do I create new destinations"
slug: /how-do-i-create-new-destinations
---

In ActiveMQ you do **not** have to create destinations up front before you can use them. The ActiveMQ broker auto-creates the physical resources associated with a destination on demand (i.e. when messages are sent to a new destination on a broker).

This means that a client can create a new Queue or Topic dynamically either by

*   calling createQueue() or createTopic() on a JMS Session
*   creating an instance of ActiveMQTopic or ActiveMQQueue and possibly registering them in JNDI

and the broker will automatically create the physical destinations for you. This avoids spending large amounts of time creating every individual destination you wish to use and possibly then exposing it in some JNDI provider.

In addition, if you are using JNDI to lookup connection factory and destinations, you can specify which destinations you wish to create by default using the properties file. See the [JNDI Support](jndi-support) for more details.

### Creating destinations on startup

Its completely optional but as of 4.1 you can [Configure Startup Destinations](configure-startup-destinations) to specify which destinations are automatically created when the broker starts.

### Limiting creation of destinations

If you need to restrict access or creation of destinations then please use the [Security](security) option to disable destinations being auto-created for certain users.

Client side destination objects are not the same as server side resources

Note that the ActiveMQ Broker will only create server side resources for destinations when messages are actually sent to them. So you can create as many instances of ActiveMQTopic and ActiveMQQueue on a client without any real overhead until you actually send messages to them on a broker. So a JMS client creating a new ActiveMQQueue POJO does not mean you are creating server side queue resources.

Think of the ActiveMQQueue and ActiveMQTopic classes as like java.net.URL. They are just names which refer to server side resources which are auto-created when they are used.  
This means that different clients creating different ActiveMQQueue instances will communicate with the same physical queue on a JMS broker if the name is the same.

You can browse the currently available queues and topics using [JMX](jmx). You can also use the [JMX](jmx) MBeans to create the destinations you require.

