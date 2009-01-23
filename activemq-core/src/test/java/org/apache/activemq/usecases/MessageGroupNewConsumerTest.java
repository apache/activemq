
package org.apache.activemq.usecases;

import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import junit.framework.TestCase;

/*
 * Test plan:
 * Producer: publish messages into a queue, with three message groups
 * Consumer1: created before any messages are created
 * Consumer2: created after consumer1 has processed one message from each message group
 * 
 * All three groups are handled by to consumer1, so consumer2 should not get any messages.
 * See bug AMQ-2016: Message grouping fails when consumers are added
 */
public class MessageGroupNewConsumerTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(MessageGroupNewConsumerTest.class);
    private Connection connection;
    // Released after the messages are created
    private CountDownLatch latchMessagesCreated = new CountDownLatch(1);
    // Released after one message from each group is consumed
    private CountDownLatch latchGroupsAcquired = new CountDownLatch(1);

    private static final String[] groupNames = { "GrA", "GrB", "GrC" };
    private int messagesSent, messagesRecvd1, messagesRecvd2;
    // with the prefetch too high, this bug is not realized
    private static final String connStr = "vm://localhost?broker.persistent=false&broker.useJmx=false&jms.prefetchPolicy.all=1";

    public void testNewConsumer() throws JMSException, InterruptedException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connStr);
        connection = factory.createConnection();
        connection.start();
        final String queueName = this.getClass().getSimpleName();
        final Thread producerThread = new Thread() {
            public void run() {
                try {
                    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    Queue queue = session.createQueue(queueName);
                    MessageProducer prod = session.createProducer(queue);
                    for (int i=0; i<10; i++) {
                        for(String group : groupNames) {
                            Message message = generateMessage(session, group, i+1);
                            prod.send(message);
                            session.commit();
                            messagesSent++;
                        }
                        LOG.info("Sent message seq "+ (i+1));
                        if (i==0) {
                            latchMessagesCreated.countDown();
                        }
                        if (i==2) {
                            LOG.info("Prod: Waiting for groups");
                            latchGroupsAcquired.await();
                        }
                        Thread.sleep(20);
                    }
                    LOG.info(messagesSent+" messages sent");
                    prod.close();
                    session.close();
                } catch (Exception e) {
                    LOG.error("Producer failed", e);
                }
            }
        };
        final Thread consumerThread1 = new Thread() {
            public void run() {
                try {
                    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    Queue queue = session.createQueue(queueName);
                    MessageConsumer con1 = session.createConsumer(queue);
                    latchMessagesCreated.await();
                    while(true) {
                        Message message = con1.receive(1000);
                        if (message == null)  break;
                        LOG.info("Con1 got message "+formatMessage(message));
                        session.commit();
                        messagesRecvd1++;
                        // since we get the messages in order, the first few messages will be one from each group
                        // after we get one from each group, start the other consumer
                        if (messagesRecvd1 == groupNames.length) {
                            LOG.info("All groups acquired");
                            latchGroupsAcquired.countDown();
                            Thread.sleep(1000);
                        }
                        Thread.sleep(50);
                    }
                    LOG.info(messagesRecvd1+" messages received by consumer1");
                    con1.close();
                    session.close();
                } catch (Exception e) {
                    LOG.error("Consumer 1 failed", e);
                }
            }
        };
        final Thread consumerThread2 = new Thread() {
            public void run() {
                try {
                    latchGroupsAcquired.await();
                    while(consumerThread1.isAlive()) {
                        LOG.info("(re)starting consumer2");
                        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                        Queue queue = session.createQueue(queueName);
                        MessageConsumer con2 = session.createConsumer(queue);
                        while(true) {
                            Message message = con2.receive(500);
                            if (message == null) break;
                            LOG.info("Con2 got message       "+formatMessage(message));
                            session.commit();
                            messagesRecvd2++;
                            Thread.sleep(50);
                        }
                        con2.close();
                        session.close();
                    }
                    LOG.info(messagesRecvd2+" messages received by consumer2");
                } catch (Exception e) {
                    LOG.error("Consumer 2 failed", e);
                }
            }
        };
        consumerThread2.start();
        consumerThread1.start();
        producerThread.start();
        // wait for threads to finish
        producerThread.join();
        consumerThread1.join();
        consumerThread2.join();
        connection.close();
        // check results
        assertEquals("consumer 2 should not get any messages", 0, messagesRecvd2);
        assertEquals("consumer 1 should get all the messages", messagesSent, messagesRecvd1);
        assertTrue("producer failed to send any messages", messagesSent > 0);
    }

    public Message generateMessage(Session session, String groupId, int seq) throws JMSException {
        TextMessage m = session.createTextMessage();
        m.setJMSType("TEST_MESSAGE");
        m.setStringProperty("JMSXGroupID", groupId);
        m.setIntProperty("JMSXGroupSeq", seq);
        m.setText("<?xml?><testMessage/>");
        return m;
    }
    public String formatMessage(Message m) {
        try {
            return m.getStringProperty("JMSXGroupID")+"-"+m.getIntProperty("JMSXGroupSeq")+"-"+m.getBooleanProperty("JMSXGroupFirstForConsumer");
        } catch (Exception e) {
            return e.getClass().getSimpleName()+": "+e.getMessage();
        }
    }
}
