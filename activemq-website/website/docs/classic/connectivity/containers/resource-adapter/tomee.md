---
title: "TomEE"
slug: /tomee
---


Apache TomEE is a distribution of Tomcat with fully integrated ActiveMQ offering full JMS support to plain war files, Servlets and more. No setup is required and code like the following will work out of the box.

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

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        //...

        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create a MessageProducer from the Session to the Topic or Queue
        MessageProducer producer = session.createProducer(fooTopic);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Create a message
        TextMessage message = session.createTextMessage("Hello World!");

        // Tell the producer to send the message
        producer.send(message);

        //...
    }
}

All topics and queues are created automatically when declared properly in code as above. For example, the following reference in code:

    @Resource(name = "foo")
    private Topic fooTopic;

Is identical to the following declaration in the <tomee.home>/conf/tomee.xml file or WEB-INF/resources.xml file:

    <Resource id="foo" type="javax.jms.Topic" />

In both situations Topic name would be 'foo'.

Configuring an Embedded Broker
------------------------------

If no broker is configured and JMS is used in the webapp, TomEE will create a broker equivalent to the following declaration:

<tomee>
    <Resource id="MyJmsResourceAdapter" type="ActiveMQResourceAdapter">
        BrokerXmlConfig =  broker:(tcp://someHostName:61616)
        ServerUrl       =  vm://localhost
    </Resource>

    <Resource id="MyJmsConnectionFactory" type="javax.jms.ConnectionFactory">
        ResourceAdapter = MyJmsResourceAdapter
    </Resource>
</tomee>

This creates an ActiveMQ Broker that runs inside TomEE and is bound to the address `someHostName:61616`. This broker will start when TomEE starts and shutdown when TomEE shuts down. All JMS communication happening inside TomEE itself will happen using the embedded ActiveMQ Broker over the ActiveMQ "vm" transport which is optimized for sending and receiving messages when in the same JVM as the Broker.

Connecting to an External Broker
--------------------------------

It is possible to connect to an ActiveMQ Broker running externally. The setup is similar to the above with only two changes.

<tomee>
    <Resource id="MyJmsResourceAdapter" type="ActiveMQResourceAdapter">
        BrokerXmlConfig =
        ServerUrl       =  tcp://someHostName:61616
    </Resource>

    <Resource id="MyJmsConnectionFactory" type="javax.jms.ConnectionFactory">
        ResourceAdapter = MyJmsResourceAdapter
    </Resource>
</tomee>

Here the `BrokerXmlConfig` property is empty as we are not configuring an embedded broker to be run inside TomEE. The `ServerUrl` now points to an explicit remote host and port `someHostName:61616` where an ActiveMQ Broker must be running. This Broker can be a plain ActiveMQ install or an ActiveMQ Broker embedded in a TomEE install.

