---
title: "JMS Bridge With Oracle AQ"
description: "Bridge Oracle AQ queues and topics into ActiveMQ Classic using Camel routes and AQjmsFactory connection factories, plus PL/SQL to create the Oracle AQ queue."
slug: /jms-bridge-with-oracle-aq
---


### Example of a configuration that shows how to connect to Oracle AQ queues and topics.
```
<beans>
    <!-- Allows us to use system properties as variables in this configuration file -->
    <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

    <broker useJmx="true" persistent="false" xmlns="http://activemq.org/config/1.0"
            brokerName="localhost" dataDirectory="${activemq.base}/data" >
    </broker>

    <camelContext id="camel" xmlns="http://activemq.apache.org/camel/schema/spring">
        <!-- Dependencies: ojdbc.jar and aqjms.jar must be in the activemq lib directory -->

        <!-- this camel route will read incoming messages from Oracle -->
        <route>
            <from uri="oracleQueue:queue:ORACLE_QUEUE">
            <to   uri="activemq:queue:queue.inboundOracleAQqueue" >
        </route>
        <route>
            <!-- NOTE: I have had success with a topic using ActiveMQ 5.3, but not 5.1 -->
            <from uri="oracleTopic:topic:ORACLE_TOPIC">
            <to   uri="activemq:queue:queue.inboundOracleAQtopic" >
        </route>

        <!-- these camel routes will log the messages to the console .... replace them with something more useful!!  -->
        <route>
            <from uri="activemq:queue:queue.inboundOracleAQqueue" >
            <to uri="log:oracleAQ.inbound.got_a_queue_message?level=ERROR">
        </route>
        <route>
            <from uri="activemq:queue:queue.inboundOracleAQtopic" >
            <to uri="log:oracleAQ.inbound.got_a_topic_message?level=ERROR">
        </route>
    </camelContext>

    <!-- without the following bean instantiation, we will get an oracle.jms.AQjmsException with each and every received message -->
    <bean id="requiredBeanForOracleAq" class="org.apache.activemq.ActiveMQConnectionFactory" />

    <bean id="connectionFactoryOracleAQQueue" class="oracle.jms.AQjmsFactory" factory-method="getQueueConnectionFactory">
        <constructor-arg index="0">
            <value>jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST ....... (SERVICE_NAME = myDbService)))</value>
        </constructor-arg>
        <constructor-arg index="1" type="java.util.Properties">
            <value></value>
        </constructor-arg>
    </bean>

    <bean id="connectionFactoryOracleAQTopic" class="oracle.jms.AQjmsFactory"
                factory-method="getQueueConnectionFactory">
        <constructor-arg index="0">
            <value>jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST ....... (SERVICE_NAME = myDbService)))</value>
        </constructor-arg>
        <constructor-arg index="1" type="java.util.Properties">
            <value></value>
        </constructor-arg>
    </bean>

    <bean id="oracleQueueCredentials" class="org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter">
        <property name="targetConnectionFactory">
            <ref bean="connectionFactoryOracleAQQueue">
        </property>
        <property name="username">
            <value>foo</value>
        </property>
        <property name="password">
            <value>bar</value>
        </property>
    </bean>

    <bean id="oracleTopicCredentials" class="org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter">
        <property name="targetConnectionFactory">
            <ref bean="connectionFactoryOracleAQTopic">
        </property>
        <property name="username">
            <value>foo</value>
        </property>
        <property name="password">
            <value>bar</value>
        </property>
    </bean>

   <bean id="oracleQueue" class="org.apache.camel.component.jms.JmsComponent">
        <property name="connectionFactory" ref="oracleQueueCredentials">
   </bean>

   <bean id="oracleTopic" class="org.apache.camel.component.jms.JmsComponent">
        <property name="connectionFactory" ref="oracleTopicCredentials">
   </bean>
</beans>
```
If you run in an OSGi environment such as [ServiceMix 4](http://servicemix.apache.org) then take a look at [this discussion](http://servicemix.396122.n5.nabble.com/ServiceMix-4-2-Camel-OracleAQ-td421214.html#a421214) how to install the OracleAQ Client in the OSGi container.

#### Oracle SQL code

You may need to setup OracleAQ, and here is a sample code
```
BEGIN
 DBMS_AQADM.CREATE_QUEUE_TABLE( queue_table => 'queue_message_table', queue_payload_type => 'SYS.AQ$_JMS_TEXT_MESSAGE');
END;        

BEGIN
  DBMS_AQADM.CREATE_QUEUE( queue_name => 'ORACLE_QUEUE', queue_table => 'queue_message_table');
END;

BEGIN
  DBMS_AQADM.START_QUEUE(queue_name => 'ORACLE_QUEUE');
END;  
```
You can also find more information about OracleAQ and using JMS at: [http://docs.oracle.com/cd/B13789_01/server.101/b10785/jm_exmpl.htm](http://docs.oracle.com/cd/B13789_01/server.101/b10785/jm_exmpl.htm)

