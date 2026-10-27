---
title: "BlazeDS"
slug: /blazeds
---


BlazeDS
-------

You may also want to check out the [Simplified BlazeDS and JMS article](http://mmartinsoftware.blogspot.com/2008/05/simplified-blazeds-and-/FAQ/jms) by [Michael Martin](http://mmartinsoftware.blogspot.com/).

Using the dynamicQueues feature of the [JNDI Support](jndi-support) Ryan Gardner created thisworking BlazeDS messaging-config.xml file:
```
<?xml version="1.0" encoding="UTF-8"?>
<service id="message-service" class="flex.messaging.services.MessageService">

   <adapters>
       <adapter-definition id="actionscript" class="flex.messaging.services.messaging.adapters.ActionScriptAdapter" default="true" />
       <adapter-definition id="jms" class="flex.messaging.services.messaging.adapters.JMSAdapter"/>
   </adapters>

   <default-channels>
               <channel ref="my-streaming-amf"/>
               <channel ref="my-polling-amf"/>
   </default-channels>

  <destination id="inbound-sms-destination">

   <properties>
   <jms>
     <destination-type>Topic</destination-type>
     <message-type>javax.jms.TextMessage</message-type>
     <connection-factory>topicConnectionFactory</connection-factory>
     <destination-jndi-name>dynamicTopics/SMSReturnMessages</destination-jndi-name>

     <delivery-mode>NON_PERSISTENT</delivery-mode>
     <message-priority>DEFAULT_PRIORITY</message-priority>
     <acknowledge-mode>AUTO_ACKNOWLEDGE</acknowledge-mode>
     <initial-context-environment>
       <property>
         <name>Context.INITIAL\_CONTEXT\_FACTORY</name>
         <value>org.apache.activemq.jndi.ActiveMQInitialContextFactory</value>
       </property>
       <property>
         <name>Context.PROVIDER_URL</name>
         <value>vm://localhost</value>

       </property>
     </initial-context-environment>
   </jms>
   </properties>
   <adapter ref="jms"/>
   </destination>
</service>
```
