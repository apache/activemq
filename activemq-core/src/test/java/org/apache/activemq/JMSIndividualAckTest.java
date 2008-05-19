package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @version $Revision: 1.4 $
 */
public class JMSIndividualAckTest extends TestSupport {

    private Connection connection;

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    /**
     * Tests if acknowledged messages are being consumed.
     *
     * @throws JMSException
     */
    public void testAckedMessageAreConsumed() throws JMSException {
        connection.start();
        Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        // Reset the session.
        session.close();
        session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createConsumer(queue);
        msg = consumer.receive(1000);
        assertNull(msg);

        session.close();
    }

    /**
     * Tests if acknowledged messages are being consumed.
     *
     * @throws JMSException
     */
    public void testLastMessageAcked() throws JMSException {
        connection.start();
        Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageProducer producer = session.createProducer(queue);
        TextMessage msg1 = session.createTextMessage("msg1");
        TextMessage msg2 = session.createTextMessage("msg2");
        TextMessage msg3 = session.createTextMessage("msg3");
        producer.send(msg1);
        producer.send(msg2);
        producer.send(msg3);

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);        
        msg = consumer.receive(1000);
        assertNotNull(msg);
        msg.acknowledge();

        // Reset the session.
        session.close();
        session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

        // Attempt to Consume the message...
        consumer = session.createConsumer(queue);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        assertEquals(msg1,msg);
        msg = consumer.receive(1000);
        assertNotNull(msg);
        assertEquals(msg2,msg);
        msg = consumer.receive(1000);
        assertNull(msg);
        session.close();
    }
    
    /**
     * Tests if unacknowledged messages are being re-delivered when the consumer connects again.
     * 
     * @throws JMSException
     */
    public void testUnAckedMessageAreNotConsumedOnSessionClose() throws JMSException {
        connection.start();
        Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNotNull(msg);        
        // Don't ack the message.
        
        // Reset the session.  This should cause the unacknowledged message to be re-delivered.
        session.close();
        session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
                
        // Attempt to Consume the message...
        consumer = session.createConsumer(queue);
        msg = consumer.receive(2000);
        assertNotNull(msg);        
        msg.acknowledge();
        
        session.close();
    }

    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

}
