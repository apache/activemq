---
title: "ObjectMessage"
slug: /objectmessage
---


Although ObjectMessage usage is generally discouraged, as it introduces coupling of class paths between producers and consumers, ActiveMQ supports them as part of the JMS specification.

Security
--------

ObjectMessage objects depend on Java serialization of marshal/unmarshal object payload. This process is generally considered unsafe as malicious payload can exploit the host system. That's why starting with versions **5.12.2** and **5.13.0**, ActiveMQ enforces users to explicitly whitelist packages that can be exchanged using ObjectMessages.

If you need to exchange object messages, you need to add packages your applications are using. You can do that with by using `org.apache.activemq.SERIALIZABLE_PACKAGES` system property, interpreted by the broker and the activemq client library. You can add this system property to `ACTIVEMQ_OPTS` variable in `${ACTIVEMQ_HOME}/bin/env` script.

For example:
```
-Dorg.apache.activemq.SERIALIZABLE_PACKAGES=javax.security,java.util,org.apache.activemq,org.fusesource.hawtbuf,com.thoughtworks.xstream.mapper,com.mycompany.myapp
```
will add `com.mycompany.myapp` package to the list of trusted packages. Note that other packages listed here are enabled by default as they are necessary for the regular broker work. In case you want to shortcut this mechanism, you can allow all packages to be trusted by using `*` wildcard, like
```
-Dorg.apache.activemq.SERIALIZABLE_PACKAGES=*
```
### Clients

On the client side, you need to have this same mechanism as malicious code can be deserialized on `ObjectMessage.getObject()` call, compromising your application's environment. You can use the same configuration mechanism on the broker and configure trusted classes using system properties. However, this is usually not convenient in the client applications, so in **5.12.2** and **5.13.1** we introduced additional configuration mechanism using `ActiveMQConnectionFactory`. There are two additional methods defined:

*   The `setTrustedPackages()` method allows you to set the list of trusted packages you want to be to unserialize, like
```java
ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
factory.setTrustedPackages(new ArrayList(Arrays.asList("org.apache.activemq.test,org.apache.camel.test".split(","))));
```
*   The `setTrustAllPackages()` allows you to turn off security check and trust all classes. It's useful for testing purposes.
```java
ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
factory.setTrustAllPackages(true);
```
You can set the same properties in Camel context like:

```xml
<bean id="connectionFactory" class="org.apache.activemq.spring.ActiveMQConnectionFactory">
    <property name="brokerURL" value="tcp://localhost:61616"/>
    <property name="trustedPackages">
        <list>
            <value>org.apache.activemq.test</value>
            <value>org.apache.camel.test</value>
        </list>
    </property>
</bean>
<bean id="jmsConfig" class="org.apache.camel.component.jms.JmsConfiguration">
    <property name="connectionFactory" ref="connectionFactory"/>
</bean>
<bean id="activemq" class="org.apache.activemq.camel.component.ActiveMQComponent">
    <property name="configuration" ref="jmsConfig"/>
</bean>
```

or

```xml
<bean id="connectionFactory" class="org.apache.activemq.spring.ActiveMQConnectionFactory">
    <property name="brokerURL" value="tcp://localhost:61616"/>
<property name="trustAllPackages" value="true"/>
</bean>
<bean id="jmsConfig" class="org.apache.camel.component.jms.JmsConfiguration">
    <property name="connectionFactory" ref="connectionFactory"/>
</bean>
<bean id="activemq" class="org.apache.activemq.camel.component.ActiveMQComponent">
    <property name="configuration" ref="jmsConfig"/>
</bean>
```

This configuration will override system properties if they are set.

