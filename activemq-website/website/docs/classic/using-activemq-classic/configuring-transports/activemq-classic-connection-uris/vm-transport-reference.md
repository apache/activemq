---
title: "VM Transport Reference"
description: "Embed an ActiveMQ Classic broker in the JVM with vm://brokerName, using create, waitForStart and broker.* options, the vm:(broker:...) syntax and brokerConfig."
slug: /vm-transport-reference
---

### The VM Transport

The VM transport allows clients to connect to each other inside the VM without the overhead of the network communication. The connection used is not a socket connection but use direct method invocations which enables a high performance embedded messaging system.

The first client to use the VM connection will boot an embedded broker. Subsequent connections will attach that the same broker. Once all VM connections to the broker have been closed, the embedded broker will automatically shutdown.

#### Simple Broker Configuration Syntax

This is the normal syntax for a VM connection. It's simple, but provides only a limited amount of configuration of the embedded broker.
```
vm://brokerName?transportOptions
```
If you want to connect to an already instantiated, embedded broker (e.g. as in case the case of Apache ServiceMix), make sure the brokerName used in the **vm://brokerName** url matches the brokerName of the already running broker.

##### Transport Options

Option Name|Default Value|Description
---|---|---
marshal|false|If true, forces each command sent over the transport to be marshalled and unmarshalled using a WireFormat
wireFormat|default|The name of the WireFormat to use
wireFormat.*||All the properties with this prefix are used to configure the wireFormat|
create|true|If the broker should be created on demand if it does not already exist.|
waitForStart|-1|If > 0, indicates the timeout in milliseconds to wait for a broker to start. Values -1 and 0 mean don't wait. Only supported in ActiveMQ 5.2+
broker.*||All the properties with this prefix are used to configure the broker. See [Configuring Wire Formats](configuring-wire-formats) for more information

##### Example URI
```
vm://broker1?marshal=false&broker.persistent=false
```
Be careful with embedded brokers

If you are using the VM transport and wish to explicitly configure an [Embedded Broker](how-do-i-embed-a-broker-inside-a-connection) there is a chance that you could create the JMS connections first before the broker starts up. Currently ActiveMQ will auto-create a broker if you use the VM transport and there is not one already configured. (In 5.2 it is possible to use the waitForStart and create=false options for the connection uri)

So to work around this if you are using Spring you may wish to use the **depends-on** attribute so that your JMS ConnectionFactory depends on the embedded broker to avoid this happening. e.g.
```
<bean id="broker" class="org.apache.activemq.xbean.BrokerFactoryBean">
    <property name="config" value="classpath:org/apache/activemq/xbean/activemq.xml" />
    <property name="start" value="true" />
  </bean>

  <bean id="connectionFactory" class="org.apache.activemq.ActiveMQConnectionFactory" depends-on="broker">
    <property name="brokerURL" value="vm://localhost"/>
  </bean>
```

#### Advanced Broker Configuration Syntax

This is the advanced syntax for a VM connection. It's allows you configure the broker more extensively using a [Broker Configuration URI](broker-configuration-uri).
```
vm:(broker:(tcp://localhost)?brokerOptions)?transportOptions
```
or
```
vm:broker:(tcp://localhost)?brokerOptions
```

##### Transport Options

Option Name|Default Value|Description
---|---|---
marshal|false|If true, forces each command sent over the transport to be marshalled and unmarshalled using a WireFormat
wireFormat|default|The name of the WireFormat to use
wireFormat.*||All the properties with this prefix are used to configure the wireFormat

There are [more options](how-should-i-use-the-vm-transport) on optimising the use of the VM transport.

##### Example URI
```
vm:(broker:(tcp://localhost:6000)?persistent=false)?marshal=false
```

#### Configuring an Embedded Broker Using an External Config File

 To start an embedded broker using the vm transport and configure it using an external configuration file (i.e. activemq.xml), use the following URI:
```
vm://localhost?brokerConfig=xbean:activemq.xml 
```
