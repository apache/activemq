package org.apache.activemq.web;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

import junit.framework.TestCase;

public class JettyTestSupport extends TestCase {

    BrokerService broker;
    Server server;
    ActiveMQConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;
    
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:61616");
        broker.start();
        broker.waitUntilStarted();
        
        server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(8080);
        connector.setServer(server);
        WebAppContext context = new WebAppContext();

        context.setResourceBase("src/main/webapp");
        context.setContextPath("/");
        context.setServer(server);
        server.setHandler(context);
        server.setConnectors(new Connector[] {
            connector
        });
        server.start();   
        
        factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue("test"));
    }

    protected void tearDown() throws Exception {
        server.stop();
        broker.stop();
        broker.waitUntilStopped();
        session.close();
        connection.close();
    }

    
    
}
