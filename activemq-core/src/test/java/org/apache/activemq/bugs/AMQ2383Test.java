package org.apache.activemq.bugs;


import static org.junit.Assert.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;

public class AMQ2383Test {

    @Test
    public void activeMQTest() throws Exception {
        Destination dest = ActiveMQQueue.createDestination("testQueue", ActiveMQQueue.QUEUE_TYPE);
        ConnectionFactory factory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.useJmx=false&broker.persistent=false");
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(dest);
        TextMessage sentMsg = producerSession.createTextMessage("test...");
        producer.send(sentMsg);
        producerSession.close();

        Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(dest);
        TextMessage receivedMsg = (TextMessage)consumer.receive();
        consumerSession.rollback();
        consumerSession.close();

        assertEquals(sentMsg, receivedMsg);

        Thread.sleep(10000);

        producerConnection.close();
        consumerConnection.close();
    }
}
