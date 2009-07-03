package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to determine if expired messages are being reaped if there is
 * no active consumer connected to the broker. 
 * 
 * @author bsnyder
 *
 */
public class MessageExpirationReaperTest {
    
    protected BrokerService broker; 
    protected ConnectionFactory factory;
    protected ActiveMQConnection connection;
    protected String destinationName = "TEST.Q";
    protected String brokerUrl = "tcp://localhost:61616";
    protected String brokerName = "testBroker";
    
    @Before
    public void init() throws Exception {
        createBroker();
        
        factory = createConnectionFactory();
        connection = (ActiveMQConnection) factory.createConnection();
        connection.start();
    }
    
    @After
    public void cleanUp() throws Exception {
        connection.close();
        broker.stop();
    }
    
    protected void createBroker() throws Exception {
        broker = new BrokerService();
//        broker.setPersistent(false);
//        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setBrokerName(brokerName);
        broker.addConnector(brokerUrl);
        
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(500);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        
        broker.start();
    }
    
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(brokerUrl);
    }
    
    protected Session createSession() throws Exception {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);  
    }
    
    @Test
    public void testExpiredMessageReaping() throws Exception {
        
        Session producerSession = createSession();
        ActiveMQDestination destination =  (ActiveMQDestination) producerSession.createQueue(destinationName);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setTimeToLive(1000);
        
        final int count = 3;
        // Send some messages with an expiration 
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("" + i);
            producer.send(message);
        }
        
        // Let the messages expire 
        Thread.sleep(2000);
        
        DestinationViewMBean view = createView(destination);
        
        assertEquals("Incorrect inflight count: " + view.getInFlightCount(), 0, view.getInFlightCount());
        assertEquals("Incorrect queue size count", 0, view.getQueueSize());
        assertEquals("Incorrect expired size count", 3, view.getEnqueueCount());   
        
        // Send more messages with an expiration 
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("" + i);
            producer.send(message);
        }
        
        // Let the messages expire 
        Thread.sleep(2000);
        
        // Simply browse the queue 
        Session browserSession = createSession();
        QueueBrowser browser = browserSession.createBrowser((Queue) destination);
        assertFalse("no message in the browser", browser.getEnumeration().hasMoreElements()); 
        
        // The messages expire and should be reaped because of the presence of 
        // the queue browser 
        assertEquals("Wrong inFlightCount: " + view.getInFlightCount(), 0, view.getInFlightCount());
    }
    
    protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":BrokerName=" + brokerName + ",Type=Queue,Destination=" + destinationName);
        } else {
            name = new ObjectName(domain + ":BrokerName=" + brokerName + ",Type=Topic,Destination=" + destinationName);
        }
        return (DestinationViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, name, DestinationViewMBean.class, true);
    }
}
