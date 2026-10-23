---
title: "Inbound Communication"
slug: /inbound-communication
---


Configuring an MDB to receive messages from ActiveMQ
----------------------------------------------------

There are three MDBs declared in the ejb-jar.xml deployment descriptor. For this example, I will be explaining how to configure the `TopicDurableMDB` to be invoked by JBoss when a message is received on an ActiveMQ Topic.

### The Bean

In the ejb-jar.xml deployment descriptor, the `TopicDurableMDB` is declared as follows:

**ejb-jar.xml**
```
<message-driven>
   ...
   <ejb-name>TopicDurableMDB</ejb-name>
   <ejb-class>com.panacya.platform.service.bus.mdb.SimpleMessageReceiverBean</ejb-class>
   <messaging-type>javax.jms.MessageListener</messaging-type>
   ...
   <activation-config>
      <activation-config-property>
         <activation-config-property-name>Destination</activation-config-property-name>
         <activation-config-property-value>topic.testTopic</activation-config-property-value>
      </activation-config-property>
      <activation-config-property>
         <activation-config-property-name>DestinationType</activation-config-property-name>
         <activation-config-property-value>javax.jms.Topic</activation-config-property-value>
      </activation-config-property>
      ...
   </activation-config>
   ...
</message-driven>
```
The `activation-config` element and it's child element, `activation-config-property`, are new elements for EJBs, so you might not be familiar with them. I won't go into to much detail about them, but it is important to understand that this is the first mechanism you use to link an MDB to a JCA.

### The Connector

The two `activation-config-properties` shown above link to the following elements in the [ra.xml](http://activemq.codehaus.org/checkout/activemq/modules/ra/src/rar/META-INF/ra.xml) file, which is contained within the activemq-ra-1.2.rar file:

**ra.xml**
```
<inbound-resourceadapter>
  ...
     <activationspec>
        <activationspec-class>org.activemq.ra.ActiveMQActivationSpec</activationspec-class>
        <required-config-property>
           <config-property-name>Destination</config-property-name>
        </required-config-property>
        <required-config-property>
           <config-property-name>DestinationType</config-property-name>
        </required-config-property>
     </activationspec>
  ...
</inbound-resourceadapter>
```
In the ejb-jar.xml file section shown above, the value of the `Destination` property is set to `topic.testTopic`. This value is the physical name of the ActiveMQ destination the `TopicDurableMDB` will be receiving messages from and not a JNDI name. In other words, the value of the `Destination` property has no meaning to JBoss. It is purely an ActiveMQ setting.

### The Glue

In JBoss, the thing which connects an inbound JMS destination to an MDB is a JBoss container. To use ActiveMQ as the inbound message source for the `TopicDurableMDB` we must configure a new JBoss container. We do this in the jboss.xml file.

Three things are needed in the jboss.xml file in order to tie an MDB to a connector. They are:

1.  Configure a new `invoker-proxy-binding` that declares `JBossMessageEndpointFactory` as the `proxy-factory`
2.  Configure a new MDB container which uses the new `invoker-proxy-binding`
3.  Declare which MDBs should go into the new container

This first snippet configures a new `invoker-proxy-binding`:

**jboss.xml – invoker-proxy-binding**
```
<invoker-proxy-binding>
   <name>activemq-message-driven-bean</name>
   <invoker-mbean>default</invoker-mbean>
   <proxy-factory>org.jboss.ejb.plugins.inflow.JBossMessageEndpointFactory</proxy-factory>
   ...
</invoker-proxy-binding>
```
This second snippet configures a new MDB container which uses the `invoker-proxy-binding` configured above:

**jboss.xml – container-configuration**
```
<container-configuration>
   <container-name>ActiveMQ Message Driven Bean</container-name>
   <call-logging>false</call-logging>
   <invoker-proxy-binding-name>activemq-message-driven-bean</invoker-proxy-binding-name>
   ...
</container-configuration>
```
This third snippet links the `TopicDurableMDB` to the activemq-ra-1.2.rar connector and tells JBoss to put instances of `TopicDurableMDB` into the new MDB container declared above:

**jboss.xml – TopicDurableMDB**
```
<message-driven>
   <ejb-name>TopicDurableMDB</ejb-name>
   <resource-adapter-name>activemq-ra-1.2-SNAPSHOT.rar</resource-adapter-name>
   <configuration-name>ActiveMQ Message Driven Bean</configuration-name>
</message-driven>
```
The above examples highlight the key configuration settings needed to enable MDBs deployed in JBoss to process messages from an ActiveMQ destination.

You can try the above example, plus a few more, by downloading the activemq-jboss-test.zip file which contains the complete sample project.

