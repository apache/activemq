---
title: "Outbound Communication"
slug: /outbound-communication
---


Configuring a Session Bean to send messages to ActiveMQ
-------------------------------------------------------

In the attached example application, the three MDBs use the `SenderEJB` to send JMS messages to an ActiveMQ queue. In this example, I will be explaining how to:

1.  Configure and deploy an ActiveMQ `Queue` to JBoss
2.  Configure and deploy an ActiveMQ `QueueConnectionFactory` to JBoss
3.  Configure an EJB, deployed to JBoss, to reference the above two.

### The Bean

In the ejb-jar.xml deployment descriptor, the `SenderEJB` is declared as follows:  

**ejb-jar.xml – session bean declaration**
```
<session>
   ...
   <ejb-name>SenderEJB</ejb-name>
   ...
   <ejb-class>com.panacya.platform.service.bus.sender.SenderBean</ejb-class>
   ...
   <resource-ref>
      <res-ref-name>jms/MyQueueConnectionFactory</res-ref-name>
      <res-type>javax.jms.QueueConnectionFactory</res-type>
      ...
   </resource-ref>
   <message-destination-ref>
      <message-destination-ref-name>jms/LogQueue</message-destination-ref-name>
      <message-destination-type>javax.jms.Queue</message-destination-type>
      ...
      <message-destination-link>LoggingQueue</message-destination-link>
   </message-destination-ref>
</session>
```
The `jms/MyQueueConnectionFactory` is the JNDI name the `SenderEJB` will use to lookup a `javax.jms.QueueConnectionFactory`. We will configure it to point to an ActiveMQ `QueueConnectionFactory`.

The `jms/LogQueue` is the JNDI name the `SenderEJB` will use to lookup the `javax.jms.Queue` it will send messages to. We use the `message-destination-link` element to refer to the `LoggingQueue` which is declared in the `assembly-descriptor` section of the ejb-jar.xml deployment descriptor as:  

**ejb-jar.xml – assembly descriptor section**
``` 
<assembly-descriptor>
   ...
   <message-destination>
      <message-destination-name>LoggingQueue</message-destination-name>
   </message-destination>
   ...
</assembly-descriptor>
```
This is a standard EJB deployment descriptor, nothing special.

### The Connector

The `resource-ref` element [shown above](outbound-communication), will be linked to the following element in the [ra.xml](http://activemq.codehaus.org/checkout/activemq/modules/ra/src/rar/META-INF/ra.xml) file, which is contained within the activemq-ra-1.2.rar file:  

**ra.xml – The QueueConnectionFactory**
```
<outbound-resourceadapter>
   ...
   <connection-definition>
      ...
      <connectionfactory-interface>javax.jms.QueueConnectionFactory</connectionfactory-interface>
      <connectionfactory-impl-class>org.activemq.ra.ActiveMQConnectionFactory</connectionfactory-impl-class>
      <connection-interface>javax.jms.QueueConnection</connection-interface>
      ...
   </connection-definition>
   ...
</outbound-resourceadapter>
```
The `message-destination` element [shown above](outbound-communication), will be linked to the following element in the [ra.xml](http://activemq.codehaus.org/checkout/activemq/modules/ra/src/rar/META-INF/ra.xml) file:  

**ra.xml – The Queue**
```
<adminobject>
   <adminobject-interface>javax.jms.Queue</adminobject-interface>
   <adminobject-class>org.activemq.message.ActiveMQQueue</adminobject-class>
   <config-property>
      <config-property-name>PhysicalName</config-property-name>
      <config-property-type>java.lang.String</config-property-type>
   </config-property>
</adminobject>
```

### The Glue

In JBoss, connecting the resources needed by the ejb-jar.xml file to resources provided by the [ra.xml](http://activemq.codehaus.org/checkout/activemq/modules/ra/src/rar/META-INF/ra.xml) file involves two additional files:

1.  **panacya-jms-ds.xml** - This is a JBoss data source file. It specifies which connector objects JBoss should instantiate and where in JNDI JBoss should place those objects.
2.  **jboss.xml** - This is a JBoss deployment descriptor which is contained within the panacya-mdb-test-1.0.jar file. It links resources needed by the EJBs to the JNDI names of resources available in JBoss.

##### panacya-jms-ds.xml – _The JBoss Data Source File_

This first snippet configures the `QueueConnectionFactory`, [declared above](outbound-communication), and places it in JNDI at `activemq/QueueConnectionFactory`:  

**panacya-jms-ds.xml – The QueueConnectionFactory**
```
<tx-connection-factory>
   <jndi-name>activemq/QueueConnectionFactory</jndi-name>
   <xa-transaction/>
   <rar-name>activemq-ra-1.2-SNAPSHOT.rar</rar-name>
   <connection-definition>javax.jms.QueueConnectionFactory</connection-definition>
   <security-domain-and-application>JmsXARealm</security-domain-and-application>
</tx-connection-factory>
```
This second snippet configures the `Queue`, [declared above](outbound-communication), and places it in JNDI at `activemq/queue/outbound`:  

**panacya-jms-ds.xml – The Queue**
```
<mbean code="org.jboss.resource.deployment.AdminObject" name="activemq.queue:name=outboundQueue">
   <attribute name="JNDIName">activemq/queue/outbound</attribute>
   <depends optional-attribute-name="RARName">jboss.jca:service=RARDeployment,name='activemq-ra-1.2-SNAPSHOT.rar'</depends>
   <attribute name="Type">javax.jms.Queue</attribute>
   <attribute name="Properties">
      PhysicalName=queue.outbound
   </attribute>
</mbean>
```
In the panacya-jms-ds.xml file section shown above, the value of the `Properties` element is set to `PhysicalName=queue.outbound`. This value is the physical name of the ActiveMQ destination the `SenderEJB` will be sending messages to and not a JNDI name. In other words, the value of the `PhysicalName` property has no meaning to JBoss. It is purely an ActiveMQ setting.

##### jboss.xml – _The JBoss Deployment Descriptor_

This first snippet links the `[jms/MyQueueConnectionFactory](outbound-communication)` JNDI name used by the `SenderEJB` to the resource name `queuefactoryref` which is local to the jboss.xml file:  

**jboss.xml – The QueueConnectionFactory for the SenderEJB**
```
<enterprise-beans>
   <session>
      <ejb-name>SenderEJB</ejb-name>
      <resource-ref>
         <res-ref-name>jms/MyQueueConnectionFactory</res-ref-name>
         <resource-name>queuefactoryref</resource-name>
      </resource-ref>
   </session>
   ...
</enterprise-beans>
```
This second snippet links the local `queuefactoryref` name to the global JNDI name `java:/activemq/QueueConnectionFactory` which was [declared above](outbound-communication):  

**jboss.xml – Linking queuefactoryref to the global JNDI namespace**
```
<resource-managers>
   <resource-manager>
      <res-name>queuefactoryref</res-name>
      <res-jndi-name>java:/activemq/QueueConnectionFactory</res-jndi-name>
   </resource-manager>
   ...
</resource-managers>
```
This third snippet links the `LoggingQueue`, which was [declared](outbound-communication) in the `assembly-descriptor` section of the ejb-jar.xml, to the global JNDI name `activemq/queue/outbound` which was [declared above](outbound-communication):  

**jboss.xml – Linking LoggingQueue to the global JNDI namespace**
```
<assembly-descriptor>
   <message-destination>
      <message-destination-name>LoggingQueue</message-destination-name>
      <jndi-name>activemq/queue/outbound</jndi-name>
   </message-destination>
</assembly-descriptor>
```
The above example highlights the key configuration settings needed to enable EJBs deployed in JBoss to send JMS messages to an ActiveMQ destination.

You can try the above example, plus a few more, by downloading the activemq-jboss-test.zip file which contains the complete sample project.
