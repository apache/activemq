package org.apache.activemq.bugs;


import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.log4j.Logger;


public class JmsTimeoutTest extends EmbeddedBrokerTestSupport {

		private final static Logger logger = Logger.getLogger( JmsTimeoutTest.class );
	
		private int messageSize=1024*64;
		private int messageCount=10000;
		private final AtomicInteger exceptionCount = new AtomicInteger(0);
		
	    /**
	     * Test the case where the broker is blocked due to a memory limit 
	     * and a producer timeout is set on the connection.
	     * @throws Exception
	     */
	    public void testBlockedProducerConnectionTimeout() throws Exception {
	        final ActiveMQConnection cx = (ActiveMQConnection)createConnection();
	        final ActiveMQDestination queue = createDestination("testqueue");
	        
	        // we should not take longer than 5 seconds to return from send
	        cx.setSendTimeout(10000);
	        	
	        Runnable r = new Runnable() {
	            public void run() {
	                try {
	                	logger.info("Sender thread starting");
	                    Session session = cx.createSession(false, 1);
	                    MessageProducer producer = session.createProducer(queue);
	                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
	                    
	                    TextMessage message = session.createTextMessage(createMessageText());
	                    for(int count=0; count<messageCount; count++){
	                    	producer.send(message);
	                    	// Currently after the timeout producer just
	                    	// returns but there is no way to know that
	                    	// the send timed out
	                    }	  
	                    logger.info("Done sending..");
	                } catch (JMSException e) {
	                    e.printStackTrace();
	                	exceptionCount.incrementAndGet();
	                	return;
	                }
	            }
	        };
	        cx.start();
	        Thread producerThread = new Thread(r);
	        producerThread.start();
	        producerThread.join(30000);
	        cx.close();
	        // We should have a few timeout exceptions as memory store will fill up
	        assertTrue(exceptionCount.get() > 0);
	    }

	    protected void setUp() throws Exception {
	        bindAddress = "tcp://localhost:61616";
	        broker = createBroker();
	        broker.setDeleteAllMessagesOnStartup(true);
	        broker.getSystemUsage().getMemoryUsage().setLimit(5*1024*1024);
	        broker.getSystemUsage().setSendFailIfNoSpaceAfterTimeout(5000);
	        super.setUp();
	    }

        private String createMessageText() {
	        StringBuffer buffer = new StringBuffer();
	        buffer.append("<filler>");
	        for (int i = buffer.length(); i < messageSize; i++) {
	            buffer.append('X');
	        }
	        buffer.append("</filler>");
	        return buffer.toString();
	    }
	    
	}