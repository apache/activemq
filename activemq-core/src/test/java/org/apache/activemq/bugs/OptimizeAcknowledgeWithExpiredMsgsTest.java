package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for AMQ-3965.
 * A consumer may be stalled in case it uses optimizeAcknowledge and receives 
 * a number of messages that expire before being dispatched to application code. 
 * See AMQ-3965 for more details.
 */
public class OptimizeAcknowledgeWithExpiredMsgsTest {
	
    private final static Logger LOG = LoggerFactory.getLogger(OptimizeAcknowledgeWithExpiredMsgsTest.class);

    private static BrokerService broker = null; 
    protected static final String DATA_DIR = "target/activemq-data/";
    public final String brokerUrl =  "tcp://localhost:61614";

   
    /** 
     * Creates a broker instance and starts it.
     * 
     * @param brokerUri - transport uri of broker
     * @param brokerName - name for the broker
     * @return a BrokerService instance with transport uri and broker name set
     * @throws Exception
     */
    protected BrokerService createBroker(URI brokerUri, String brokerName) throws Exception {
        BrokerService broker = BrokerFactory.createBroker(brokerUri);
        broker.setBrokerName(brokerName);
        broker.setBrokerId(brokerName);
        broker.setDataDirectory(DATA_DIR);
        broker.setEnableStatistics(true);
        broker.setUseJmx(false);
        return broker;
    }
    
    
    @Before
    public void setUp() throws Exception {
        final String options = "?persistent=false&useJmx=false&deleteAllMessagesOnStartup=true";
        
        broker = createBroker(new URI("broker:(" + brokerUrl + ")" + options), "localhost");
        broker.start();
    	broker.waitUntilStarted();
    	
    }
    
    
    @After
    public void tearDown() throws Exception {    
        if (broker != null)
    		broker.stop();
    }
	

    /**
     * Tests for AMQ-3965
     * Creates connection into broker using optimzeAcknowledge and prefetch=100
     * Creates producer and consumer. Producer sends 45 msgs that will expire
     * at consumer (but before being dispatched to app code).
     * Producer then sends 60 msgs without expiry.
     * 
     * Consumer receives msgs using a MessageListener and increments a counter.
     * Main thread sleeps for 5 seconds and checks the counter value. 
     * If counter != 60 msgs (the number of msgs that should get dispatched
     * to consumer) the test fails. 
     */
    @Test
    public void testOptimizedAckWithExpiredMsgs() throws Exception
    {
    	
    	ActiveMQConnectionFactory connectionFactory = 
    			new ActiveMQConnectionFactory(brokerUrl + "?jms.optimizeAcknowledge=true&jms.prefetchPolicy.all=100");

        // Create JMS resources
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST.FOO");

        // ***** Consumer code ***** 
        MessageConsumer consumer = session.createConsumer(destination); 
                 
        MyMessageListener listener = new MyMessageListener();
        connection.setExceptionListener((ExceptionListener) listener);
                
        // ***** Producer Code *****
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
        TextMessage message; 
        
        consumer.setMessageListener(listener);
        listener.setDelay(100);
        
        // Produce msgs that will expire quickly
        for (int i=0; i<45; i++) {
            message = session.createTextMessage(text);
            producer.send(message,1,1,30);
            LOG.trace("Sent message: "+ message.getJMSMessageID() + 
            	" with expiry 30 msec");
        }
        // Produce msgs that don't expire
        for (int i=0; i<60; i++) {
            message = session.createTextMessage(text);
            producer.send(message);
            LOG.trace("Sent message: "+ message.getJMSMessageID() + 
            	" with no expiry.");               	
        }
        listener.setDelay(0);       

        // set exit condition
        TestExitCondition cond = new TestExitCondition(listener);
        Wait.waitFor(cond, 5000);
        
        Assert.assertTrue("Error: Some non-expired messages were not received.", listener.getCounter() >= 60);
        
        LOG.info("Received all expected messages with counter at " + listener.getCounter());
        
        // Cleanup
        LOG.info("Cleaning up.");
        producer.close();
        consumer.close();
        session.close();
        connection.close();
        listener = null;
    }
    

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }    
    }
    
    
    /**
     * Defines the exit condition for the test.
     */
    private class TestExitCondition implements Wait.Condition {

    	private MyMessageListener listener;
    	
    	public TestExitCondition(MyMessageListener l) {
    		this.listener = l;
    	}
    	
		public boolean isSatisified() throws Exception {
    		return listener.getCounter() == 36;
		}
    	
    }
    
    
    /** 
     * Standard JMS MessageListener
     */
    private class MyMessageListener implements MessageListener, ExceptionListener {
    	
    	private AtomicInteger counter = new AtomicInteger(0);
    	private int delay = 0;
    	
    	public void onMessage(final Message message) { 
            try { 
                LOG.trace("Got Message " + message.getJMSMessageID()); 
                LOG.debug("counter at " + counter.incrementAndGet());
                if (delay>0) {
                	sleep(delay);
                }
            } catch (final Exception e) { 
                e.printStackTrace(); 
            }
        } 
    	
    	public int getCounter() {
    		return counter.get();
    	}
    	
    	public int getDelay() {
    		return delay;
    	}
    	
    	public void setDelay(int newDelay) {
    		this.delay = newDelay;
    	}
    	
    	public synchronized void onException(JMSException ex) {
            LOG.error("JMS Exception occured.  Shutting down client.");
        }
    }
}
 