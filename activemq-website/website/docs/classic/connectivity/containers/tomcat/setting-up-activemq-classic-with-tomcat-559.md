---
title: "Setting up ActiveMQ with Tomcat 5.5.9"
slug: /setting-up-activemq-classic-with-tomcat-559
---


Create the file <webapp-root>/META-INF/context.xml. Here is an example:
```
<Context antiJARLocking="true">
    <Resource
        name="jms/ConnectionFactory"
        auth="Container"
        type="org.apache.activemq.ActiveMQConnectionFactory"
        description="JMS Connection Factory"
        factory="org.apache.activemq.jndi.JNDIReferenceFactory"
        brokerURL="tcp://localhost:61616"
        brokerName="LocalActiveMQBroker"
        useEmbeddedBroker="false"/>

    <Resource name="jms/topic/MyTopic"
        auth="Container"
        type="org.apache.activemq.command.ActiveMQTopic"
        factory="org.apache.activemq.jndi.JNDIReferenceFactory"
        physicalName="MY.TEST.FOO"/>
    <Resource name="jms/queue/MyQueue"
        auth="Container"
        type="org.apache.activemq.command.ActiveMQQueue"
        factory="org.apache.activemq.jndi.JNDIReferenceFactory"
        physicalName="MY.TEST.FOO.QUEUE"/>
</Context>
```
This will setup the JNDI for the ConectionFactory and Topic to work within Tomcat.

Here is some example code that will publish a test message to the MY.TEST.FOO Topic:
```
try {
    InitialContext initCtx = new InitialContext();
    Context envContext = (Context) initCtx.lookup("java:comp/env");
    ConnectionFactory connectionFactory = (ConnectionFactory) envContext.lookup("jms/ConnectionFactory");
    Connection connection = connectionFactory.createConnection();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer producer = session.createProducer((Destination) envContext.lookup("jms/topic/MyTopic"));

    Message testMessage = session.createMessage();
    testMessage.setStringProperty("testKey", "testValue");
    producer.send(testMessage);
} catch (NamingException e) {
    // TODO handle exception
} catch (JMSException e) {
    // TODO handle exception
}
```
