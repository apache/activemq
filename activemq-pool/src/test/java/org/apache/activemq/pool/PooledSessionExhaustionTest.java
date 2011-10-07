package org.apache.activemq.pool;

import org.apache.activemq.*;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

import junit.framework.*;
import javax.jms.*;
import javax.jms.Message;

import org.apache.log4j.Logger;

public class PooledSessionExhaustionTest extends TestCase {
    private static final String QUEUE = "FOO";
    private static final int NUM_MESSAGES = 700;

    private Logger logger = Logger.getLogger(getClass());

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private String connectionUri;
    private int numReceived = 0;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
        pooledFactory = new PooledConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
        pooledFactory.setBlockIfSessionPoolIsFull(false);
    }

    public void sendMessages(ConnectionFactory connectionFactory) throws Exception {
        for (int i = 0; i < NUM_MESSAGES; i++) {
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE);
            MessageProducer producer = session.createProducer(destination);

            String msgTo = "hello";
            TextMessage message = session.createTextMessage(msgTo);
            producer.send(message);
            connection.close();
            logger.debug("sent " + i + " messages using " + connectionFactory.getClass());
        }
    }

    public void testCanExhaustSessions() throws Exception {
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);
                    Connection connection = connectionFactory.createConnection();
                    connection.start();

                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = session.createQueue(QUEUE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    for (int i = 0; i < NUM_MESSAGES; ++i) {
                        Message msg = consumer.receive(5000);
                        if (msg == null) {
                            return;
                        }
                        numReceived++;
                        if (numReceived % 20 == 0) {
                            logger.debug("received " + numReceived + " messages ");
                            System.runFinalization();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();

        sendMessages(pooledFactory);
        thread.join();

        assertEquals(NUM_MESSAGES, numReceived);
    }
}
