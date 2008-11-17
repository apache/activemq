package org.apache.activemq.usecases;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

public class NoDuplicateOnTopicNetworkTest extends TestCase {
    private static final Log LOG = LogFactory
            .getLog(NoDuplicateOnTopicNetworkTest.class);

    private static final String MULTICAST_DEFAULT = "multicast://default";
    private static final String BROKER_1 = "tcp://localhost:61626";
    private static final String BROKER_2 = "tcp://localhost:61636";
    private static final String BROKER_3 = "tcp://localhost:61646";
    private BrokerService broker1;
    private BrokerService broker2;
    private BrokerService broker3;

    private boolean dynamicOnly = false;
    // no duplicates in cyclic network if networkTTL <=1
    // when > 1, subscriptions perculate around resulting in duplicates as there is no
    // memory of the original subscription.
    // solution for 6.0 using org.apache.activemq.command.ConsumerInfo.getNetworkConsumerIds()
    private int ttl = 1;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        broker3 = createAndStartBroker("broker3", BROKER_3);
        Thread.sleep(3000);
        broker2 = createAndStartBroker("broker2", BROKER_2);
        Thread.sleep(3000);
        broker1 = createAndStartBroker("broker1", BROKER_1);
        Thread.sleep(1000);
    }

    private BrokerService createAndStartBroker(String name, String addr)
            throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(name);
        broker.addConnector(addr).setDiscoveryUri(new URI(MULTICAST_DEFAULT));
        broker.setUseJmx(false);

        NetworkConnector networkConnector = broker
                .addNetworkConnector(MULTICAST_DEFAULT);
        networkConnector.setDecreaseNetworkConsumerPriority(true);
        networkConnector.setDynamicOnly(dynamicOnly);
        networkConnector.setNetworkTTL(ttl);

        broker.start();
       
        return broker;
    }

    @Override
    protected void tearDown() throws Exception {
        broker1.stop();
        broker2.stop();
        broker3.stop();
        super.tearDown();
    }

    public void testProducerConsumerTopic() throws Exception {
        final String topicName = "broadcast";
        Thread producerThread = new Thread(new Runnable() {
            public void run() {
                TopicWithDuplicateMessages producer = new TopicWithDuplicateMessages();
                producer.setBrokerURL(BROKER_1);
                producer.setTopicName(topicName);
                try {
                    producer.produce();
                } catch (JMSException e) {
                    fail("Unexpected " + e);
                }
            }
        });

        final TopicWithDuplicateMessages consumer = new TopicWithDuplicateMessages();
        Thread consumerThread = new Thread(new Runnable() {
            public void run() {
                consumer.setBrokerURL(BROKER_2);
                consumer.setTopicName(topicName);
                try {
                    consumer.consumer();
                    consumer.getLatch().await(60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    fail("Unexpected " + e);
                }
            }
        });

        consumerThread.start();
        Thread.sleep(1000);
        producerThread.start();
        producerThread.join();
        consumerThread.join();

        Map<String, String> map = new HashMap<String, String>();
        for (String msg : consumer.getMessageStrings()) {
            assertTrue("is not a duplicate: " + msg, !map.containsKey(msg));
            map.put(msg, msg);
        }
        assertEquals("got all required messages: " + map.size(), consumer
                .getNumMessages(), map.size());
    }

    class TopicWithDuplicateMessages {
        private String brokerURL;
        private String topicName;
        private Connection connection;
        private Session session;
        private Topic topic;
        private MessageProducer producer;
        private MessageConsumer consumer;

        private List<String> receivedStrings = new ArrayList<String>();
        private int numMessages = 10;
        private CountDownLatch recievedLatch = new CountDownLatch(numMessages);

        public CountDownLatch getLatch() {
            return recievedLatch;
        }

        public List<String> getMessageStrings() {
            return receivedStrings;
        }

        public String getBrokerURL() {
            return brokerURL;
        }

        public void setBrokerURL(String brokerURL) {
            this.brokerURL = brokerURL;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        private void createConnection() throws JMSException {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    brokerURL);
            connection = factory.createConnection();
        }

        private void createTopic() throws JMSException {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            topic = session.createTopic(topicName);
        }

        private void createProducer() throws JMSException {
            producer = session.createProducer(topic);
        }

        private void createConsumer() throws JMSException {
            consumer = session.createConsumer(topic);
            consumer.setMessageListener(new MessageListener() {

                public void onMessage(Message arg0) {
                    TextMessage msg = (TextMessage) arg0;
                    try {
                        LOG.debug("Received message [" + msg.getText() + "]");
                        receivedStrings.add(msg.getText());
                        recievedLatch.countDown();
                    } catch (JMSException e) {
                        fail("Unexpected :" + e);
                    }
                }

            });
        }

        private void publish() throws JMSException {
            for (int i = 0; i < numMessages; i++) {
                TextMessage textMessage = session.createTextMessage();
                String message = "message: " + i;
                LOG.debug("Sending message[" + message + "]");
                textMessage.setText(message);
                producer.send(textMessage);
            }
        }

        public void produce() throws JMSException {
            createConnection();
            createTopic();
            createProducer();
            connection.start();
            publish();
        }

        public void consumer() throws JMSException {
            createConnection();
            createTopic();
            createConsumer();
            connection.start();
        }

        public int getNumMessages() {
            return numMessages;
        }
    }
}
