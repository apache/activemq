---
title: "Tomcat"
slug: /tomcat
---


#### Configuration issues for Tomcat 7 and later

Tomcat needs to be configured to ignore Jetty SCI annotations so that the Jetty WebSocket ServerContainerInitializer class is not inadvertently picked up by Tomcat. For more information on this problem see [AMQ-6154](https://issues.apache.org/jira/browse/AMQ-6154) and [https://wiki.apache.org/tomcat/HowTo/FasterStartUp](https://wiki.apache.org/tomcat/HowTo/FasterStartUp) and consult the Tomcat documentation for the version you are using to properly exclude the Jetty jar files from being scanned by Tomcat.

Pre-integrated Tomcat with ActiveMQ
-----------------------------------

Apache TomEE is a distribution of [Tomcat with fully integrated ActiveMQ](http://tomee.apache.org/tomcat-/FAQ/jms). All integration steps detailed here have already been done. The stack supports injection of Topic, Queue and ConnectionFactory references as well as transactional sending and delivery.

Something like the following works out of the box with no configuration:

import javax.annotation.Resource;
import javax.servlet.http.HttpServlet;
import javax.jms.Topic;
import javax.jms.Queue;
import javax.jms.ConnectionFactory;

public class MyServet extends HttpServlet {

    @Resource(name = "foo")
    private Topic fooTopic;

    @Resource(name = "bar")
    private Queue barQueue;

    @Resource
    private ConnectionFactory connectionFactory;

Manually integrating Tomcat and ActiveMQ
----------------------------------------

Note, manually integrating ActiveMQ with Tomcat does allow for Topic, Queue, and ConnectionFactory injection but does not support transactional sending and delivery.

You should go to Tomcat documentation and read JNDI Resources HOW-TO, especially part: Configure Tomcat's Resource Factory.

ActiveMQ has ready JNDI resource factory for all its administered objects: ConnectionFactory and destinations.

You must provide it as a parameter factory for your resources:

<Context ...>
  ...
  <Resource name="jms/ConnectionFactory" auth="Container"
            type="org.apache.activemq.ActiveMQConnectionFactory"/>
  <ResourceParams name="jms/ConnectionFactory">
    <parameter>
      <name>factory</name>
      <value>org.activemq.jndi.JNDIReferenceFactory</value>
    </parameter>
    <parameter>
      <name>brokerURL</name>
      <value>vm://localhost</value>
    </parameter>
    <parameter>
      <name>brokerName</name>
      <value>LocalActiveMQBroker</value>
    </parameter>
    <parameter>
      <name>useEmbeddedBroker</name>
      <value>true</value>
    </parameter>
  </ResourceParams>
  ...
</Context>

If you are using Tomcat 5.5 or later then try this instead...

<Context>
    ...
  <Resource name="jms/ConnectionFactory" auth="Container" type="org.apache.activemq.ActiveMQConnectionFactory" description="JMS Connection Factory"
        factory="org.apache.activemq.jndi.JNDIReferenceFactory" brokerURL="vm://localhost" brokerName="LocalActiveMQBroker"/>
    ....
</Context>

Also, don't forget to put ActiveMQ and dependent jars to tomcat shared lib directory.

### Creating destinations in Tomcat 5.5 or later

This is completely untested but should work

<Context>
    ...
  <Resource name="jms/someTopic" auth="Container" type="org.apache.activemq.command.ActiveMQTopic" description="my Topic"
        factory="org.apache.activemq.jndi.JNDIReferenceFactory" physicalName="FOO.BAR"/>

  <Resource name="jms/aQueue" auth="Container" type="org.apache.activemq.command.ActiveMQQueue" description="my Queue"
        factory="org.apache.activemq.jndi.JNDIReferenceFactory" physicalName="FOO.BAR"/>
    ....
</Context>

JMX Tip when working with Tomcat

If you want to use JMX with Tomcat and then connect via JConsole to view the [JMX](jmx) MBeans of the server then set the following

CATALINA\_OPTS="$CATALINA\_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=1099 \
    -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"

