package org.apache.activemq.bugs;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.bugs.AMQ1866.ConsumerThread;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OutOfOrderTestCase extends TestCase {
	
	private static final Log log = LogFactory.getLog(OutOfOrderTestCase.class);
	
	public static final String BROKER_URL = "tcp://localhost:61616";
	private static final int PREFETCH = 10;
	private static final String CONNECTION_URL = BROKER_URL + "?jms.prefetchPolicy.all=" + PREFETCH;

	public static final String QUEUE_NAME = "QUEUE";
	private static final String DESTINATION = "QUEUE?consumer.exclusive=true";
	
	BrokerService brokerService;
	Session session;
	Connection connection;
	
	int seq = 0;
	
	public void setUp() throws Exception {
		brokerService = new BrokerService();
		brokerService.setUseJmx(true);
		brokerService.addConnector(BROKER_URL);
		brokerService.deleteAllMessages();
		brokerService.start();
		
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(CONNECTION_URL);
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
 
	}
	

	protected void tearDown() throws Exception {
		session.close();
		connection.close();
		brokerService.stop();
	}



    public void testOrder() throws Exception {

        log.info("Producing messages 0-29 . . .");
        Destination destination = session.createQueue(DESTINATION);
        final MessageProducer messageProducer = session
                .createProducer(destination);
        try {
            for (int i = 0; i < 30; ++i) {
                final Message message = session
                        .createTextMessage(createMessageText(i));
                message.setStringProperty("JMSXGroupID", "FOO");

                messageProducer.send(message);
                log.info("sent " + toString(message));
            }
        } finally {
            messageProducer.close();
        }

        log.info("Consuming messages 0-9 . . .");
        consumeBatch();

        log.info("Consuming messages 10-19 . . .");
        consumeBatch();

        log.info("Consuming messages 20-29 . . .");
        consumeBatch();
    }
	
    protected void consumeBatch() throws Exception {
        Destination destination = session.createQueue(DESTINATION);
        final MessageConsumer messageConsumer = session.createConsumer(destination);
        try {
            for (int i = 0; i < 10; ++i) {
                final Message message = messageConsumer.receive(1000L);
                log.info("received " + toString(message));
                assertEquals("Message out of order", createMessageText(seq++), ((TextMessage) message).getText());
                message.acknowledge();
            }
        } finally {
            messageConsumer.close();
        }
    }

	private String toString(final Message message) throws JMSException {
	    String ret = "received message '" + ((TextMessage) message).getText() + "' - " + message.getJMSMessageID();
		if (message.getJMSRedelivered())
			 ret += " (redelivered)";
		return ret;
		
	}

	private static String createMessageText(final int index) {
		return "message #" + index;
	}
}