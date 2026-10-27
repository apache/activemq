---
title: "Geronimo"
slug: /geronimo
---


ActiveMQ is the default JMS provider in [Apache Geronimo](http://geronimo.apache.org).

ActiveMQ can be used both as JMS Client and a JMS Broker. This short  
article explains how to use it on a standalone client to access the  
topics/queues setup on a remote Geronimo/ActiveMQ broker.

1) Setup the queues and topics on the ActiveMQ Broker  
If you're using a standalone ActiveMQ broker, then following the instructions on  
ActiveMQ's website should be enough to setup everything.  
However, if your ActiveMQ's instance is embedded inside the J2EE Geronimo  
Application Server, creating Queues and Topics is about deploying Resource  
Adapters to your Geronimo server.  
The following deployment descriptor can be used to deploy two topics and a  
ConnectionFactory: weatherTopic and weatherRequestsTopic

```
<?xml version="1.0" encoding="UTF-8"?>
<connector xmlns="http://geronimo.apache.org/xml/ns/j2ee/connector"
    version="1.5"
    configId="weather/Topics"
    parentId="org/apache/geronimo/SystemJMS">
    <resourceadapter>
        <resourceadapter-instance>
            <resourceadapter-name>ActiveMQ RA</resourceadapter-name>
            <config-property-setting name="ServerUrl">tcp://localhost:61616</config-property-setting>
            <config-property-setting name="UserName">geronimo</config-property-setting>
            <config-property-setting name="Password">geronimo</config-property-setting>
            <workmanager>
                <gbean-link>DefaultWorkManager</gbean-link>
            </workmanager>
        </resourceadapter-instance>
        <outbound-resourceadapter>
            <connection-definition>

<connectionfactory-interface>javax.jms.ConnectionFactory</connectionfactory-interface>
                <connectiondefinition-instance>
                    <name>ConnectionFactory</name>

<implemented-interface>javax.jms.QueueConnectionFactory</implemented-interface>

<implemented-interface>javax.jms.TopicConnectionFactory</implemented-interface>
                    <connectionmanager>
                        <xa-transaction>
                            <transaction-caching/>
                        </xa-transaction>
                        <single-pool>
                            <max-size>10</max-size>

<blocking-timeout-milliseconds>5000</blocking-timeout-milliseconds>
                            <match-one/>
                        </single-pool>
                    </connectionmanager>
                    <global-jndi-name>ConnectionFactory</global-jndi-name>
                    <!--
<credential-interface>javax.resource.spi.security.PasswordCredential</credential-interface>
-->
                </connectiondefinition-instance>
            </connection-definition>
        </outbound-resourceadapter>
    </resourceadapter>
    <adminobject>
        <adminobject-interface>javax.jms.Topic</adminobject-interface>

<adminobject-class>org.codehaus.activemq.message.ActiveMQTopic</adminobject-class>
        <adminobject-instance>
            <message-destination-name>weatherTopic</message-destination-name>
            <config-property-setting
name="PhysicalName">weatherTopic</config-property-setting>
        </adminobject-instance>
    </adminobject>
    <adminobject>
        <adminobject-interface>javax.jms.Topic</adminobject-interface>

<adminobject-class>org.codehaus.activemq.message.ActiveMQTopic</adminobject-class>
        <adminobject-instance>

<message-destination-name>weatherRequestsTopic</message-destination-name>
            <config-property-setting
name="PhysicalName">weatherRequestsTopic</config-property-setting>
        </adminobject-instance>
    </adminobject>

</connector>
```

Then deploy it using Geronimo's deploy tool :

```
D:\\geronimo>java -jar bin\\deployer.jar deploy d:\\projects\\weather\\src\\resources\
geronimo-activemq.xml repository\\activemq\\rars\\activemq-ra-3.1-SNAPSHOT.rar
Username: system
Password: manager
Deployed weather/Topics
```

The geronimo.log file should now refer to these newly deployed Topics.

2) Now that the queues are available server-side, what we want is access them  
thanks to a standalone Client.  
Usually, the process is the following one :

*   Contact the J2EE naming server (port 1099, RMI) to get an JNDI InitialContext.
*   The J2EE server automatically exposes the ConnectionFactory and the Topics  
    through JNDI, so the InitialContext allows you to retrieve both the  
    ConnectionFactory and the Topics
*   Once you have your Topics, you just use them..

However, ActiveMQ's JNDI Implementation does NOT talk to the naming server. It's  
a stripped down version of a JNDI client that just allows to get Topics and  
Queues directly from a JMS instance.  
So, instead of supplying the naming server address, you have to supply the JMS  
server address.  
Most JNDI implementations use the java.naming.provider.url property to specify  
the naming server's address. ActiveMQ uses the brokerURL one. Using the  
java.naming.provider.url one instead will result in ActiveMQ trying to load the  
whole Broker.

3) So, now we have explained the process, let's detail the Spring way of doing  
things :

*   Create a bootstrap.properties file that's available in your classpath  
    ```
    jms.connectionFactoryName=ConnectionFactory  
    jms.jndiProviderUrl=tcp://localhost:61616  
    jms.jndiContextFactory=org.activemq.jndi.ActiveMQInitialContextFactory
    
    jms.weatherTopic=weatherTopic  
    jms.weatherRequestsTopic=weatherRequestsTopic
    ```
*   Now, in your Spring description file, declare the bean that will read the properties from the bootstrap.properties file
    
        <bean id="placeholderConfig" 
              class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
            <property name="location"><value>classpath:/bootstrap.properties</value></property>
        </bean>
    

*   Create a JNDI template (A Spring-specific wrapper around the JNDI InitialContext
       ```
       <bean id="jndiTemplate" class="org.springframework.jndi.JndiTemplate">
            <property name="environment">
                <props>
                    <prop key="java.naming.factory.initial">${jms.jndiContextFactory}</prop>
    
    				<!\-\- Specific to ActiveMQ -->
    
    				<!\-\- the address of the ActiveMQ broker -->
                    <prop key="brokerURL">${jms.jndiProviderUrl}</prop>
    				<!\-\- Some Topics Registration, since we are using a fake JNDI implementation -->
                    <prop key="topic.${jms.weatherTopic}">${jms.weatherTopic}</prop>
                    <prop key="topic.${jms.weatherRequestsTopic}">${jms.weatherRequestsTopic}</prop>
    			</props>
            </property>
        </bean>
        ```

*   Retrieve the ConnectionFactory from the JNDI context

    ```
    <bean id="internalJmsQueueConnectionFactory"
        class="org.springframework.jndi.JndiObjectFactoryBean">
        <property name="jndiTemplate">
            <ref bean="jndiTemplate"/>
        </property>
        <property name="jndiName">
            <value>${jms.connectionFactoryName}</value>
        </property>
    </bean>
    ```
I'm not 100% sure, but I think that you can put any Factory Name, it will just  
work. (In a scenario where the JNDI context actually contacts a naming server,  
it should match the name of the deployed ConnectionFactory)

*   Get the Topics instances from the JNDI Context
    ```
    <bean id="weatherTopic"
		class="org.springframework.jndi.JndiObjectFactoryBean"
		singleton="true">
        <property name="jndiTemplate">
            <ref bean="jndiTemplate"/>
        </property>
        <property name="jndiName">
            <value>${jms.weatherTopic}</value>
        </property>
  	</bean>
  	
    <bean id="weatherRequestTopic"
  		class="org.springframework.jndi.JndiObjectFactoryBean"
  		singleton="true">
          <property name="jndiTemplate">
              <ref bean="jndiTemplate"/>
          </property>
          <property name="jndiName">
              <value>${jms.weatherRequestsTopic}</value>
          </property>
  	</bean>
    ```
*   Now, you can reuse these Topics beans the way you want.

