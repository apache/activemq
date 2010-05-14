package org.apache.activemq.util;

import java.io.File;
import java.net.Socket;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.net.SocketFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

public abstract class HttpTestSupport extends TestCase {
    private static final Log LOG = LogFactory.getLog(HttpTestSupport.class);
    
    BrokerService broker;
    Server server;
    ActiveMQConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;
    Destination destination;
    
    protected boolean createBroker = true;
    
    final File homeDir = new File("src/main/webapp/uploads/");
    
    protected void setUp() throws Exception {
        
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
        waitForJettySocketToAccept("http://localhost:8080");
        
        if (createBroker) {
            broker = new BrokerService();
            broker.setPersistent(false);
            broker.setUseJmx(true);
            broker.addConnector("vm://localhost");
            broker.start();
            broker.waitUntilStarted();
            
            factory = new ActiveMQConnectionFactory("vm://localhost");
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue("test");
            producer = session.createProducer(destination);
            
            IOHelper.deleteFile(homeDir);
            homeDir.mkdir();
        }
    }

    protected void tearDown() throws Exception {
        server.stop();
        if (createBroker) {
            broker.stop();
            broker.waitUntilStopped();
            session.close();
            connection.close();
            IOHelper.deleteFile(homeDir);
        }
    }

    public void waitForJettySocketToAccept(String bindLocation) throws Exception {
        final URL url = new URL(bindLocation);
        assertTrue("Jetty endpoint is available", Wait.waitFor(new Wait.Condition() {

            public boolean isSatisified() throws Exception {
                boolean canConnect = false;
                try {
                    Socket socket = SocketFactory.getDefault().createSocket(url.getHost(), url.getPort());
                    socket.close();
                    canConnect = true;
                } catch (Exception e) {
                    LOG.warn("verify jetty available, failed to connect to " + url + e);
                }
                return canConnect;
            }}, 60 * 1000));
    }
    
}

