package org.apache.activemq;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

public class JmsSendReceiveWithMessageExpiration extends TestSupport {
	
	private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
           .getLog(JmsQueueSendReceiveWithMessageExpiration.class);
	
	protected int messageCount = 100;
    protected String[] data;
    protected Session session;
    protected Destination consumerDestination;
    protected Destination producerDestination;
    protected boolean durable = false;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected long timeToLive = 5000;
    protected boolean verbose = false;
    
    protected Connection connection;
    
    protected void setUp() throws Exception {
        
    	super.setUp();
        
        data = new String[messageCount];
        
        for (int i = 0; i < messageCount; i++) {
            data[i] = "Text for message: " + i + " at " + new Date();
        }
        
        connectionFactory = createConnectionFactory();        
        connection = createConnection();
        
        if (durable) {
            connection.setClientID(getClass().getName());
        }

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    
    /**
     * Sends and consumes the messages to a queue destination.
     *
     * @throws Exception
     */
    public void testConsumeExpiredQueue() throws Exception {
        
    	MessageProducer producer = createProducer(timeToLive);
        
        consumerDestination = session.createQueue(getConsumerSubject());
        producerDestination = session.createQueue(getProducerSubject());
        
        MessageConsumer consumer = createConsumer();        
        connection.start();  
        
    	for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty",data[i]);
            message.setIntProperty("intProperty",i);
        
            if (verbose) {
                if (log.isDebugEnabled()) {
                    log.debug("About to send a queue message: " + message + " with text: " + data[i]);
                }
            }
            
            producer.send(producerDestination, message);
        }
        
        Thread.sleep(timeToLive + 1000);
        
        // message should have expired.
        assertNull(consumer.receive(1000));
    }
    
    /**
     * Sends and consumes the messages to a queue destination.
     *
     * @throws Exception
     */
    public void testConsumeQueue() throws Exception {
        
    	MessageProducer producer = createProducer(0);
        
        consumerDestination = session.createQueue(getConsumerSubject());
        producerDestination = session.createQueue(getProducerSubject());
        
        MessageConsumer consumer = createConsumer();        
        connection.start();  
        
    	for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty",data[i]);
            message.setIntProperty("intProperty",i);
        
            if (verbose) {
                if (log.isDebugEnabled()) {
                    log.debug("About to send a queue message: " + message + " with text: " + data[i]);
                }
            }
            
            producer.send(producerDestination, message);
        }
        
        // message should have expired.
        assertNotNull(consumer.receive(1000));
    }
    
    /**
     * Sends and consumes the messages to a topic destination.
     *
     * @throws Exception
     */
    public void testConsumeExpiredTopic() throws Exception {
    	
    	MessageProducer producer = createProducer(timeToLive);
        
        consumerDestination = session.createTopic(getConsumerSubject());
        producerDestination = session.createTopic(getProducerSubject());
        
        MessageConsumer consumer = createConsumer();        
        connection.start(); 
        
    	for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty",data[i]);
            message.setIntProperty("intProperty",i);
        
            if (verbose) {
                if (log.isDebugEnabled()) {
                    log.debug("About to send a topic message: " + message + " with text: " + data[i]);
                }
            }
            
            producer.send(producerDestination, message);
        }
        
        Thread.sleep(timeToLive + 1000);
        
        // message should have expired.
        assertNull(consumer.receive(1000));
    }
    
    /**
     * Sends and consumes the messages to a topic destination.
     *
     * @throws Exception
     */
    public void testConsumeTopic() throws Exception {
    	
    	MessageProducer producer = createProducer(0);
        
        consumerDestination = session.createTopic(getConsumerSubject());
        producerDestination = session.createTopic(getProducerSubject());
        
        MessageConsumer consumer = createConsumer();        
        connection.start(); 
        
    	for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty",data[i]);
            message.setIntProperty("intProperty",i);
        
            if (verbose) {
                if (log.isDebugEnabled()) {
                    log.debug("About to send a topic message: " + message + " with text: " + data[i]);
                }
            }
            
            producer.send(producerDestination, message);
        }
        
        // message should have expired.
        assertNotNull(consumer.receive(1000));
    }
    
    
    
    protected MessageProducer createProducer(long timeToLive) throws JMSException {
    	MessageProducer producer = session.createProducer(null);
        producer.setDeliveryMode(deliveryMode);
        producer.setTimeToLive(timeToLive);
        
        return producer;    	
    }
    
    protected MessageConsumer createConsumer() throws JMSException {
        if (durable) {
            log.info("Creating durable consumer");
            return session.createDurableSubscriber((Topic) consumerDestination, getName());
        }
        return session.createConsumer(consumerDestination);
    }
    
    protected void tearDown() throws Exception {
        log.info("Dumping stats...");
    
        log.info("Closing down connection");

        /** TODO we should be able to shut down properly */
        session.close();
        connection.close();
    }

}
