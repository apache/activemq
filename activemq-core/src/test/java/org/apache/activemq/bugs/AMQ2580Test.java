/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.bugs;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;      
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

public class AMQ2580Test extends TestSupport {

    private static final Log LOG = LogFactory.getLog(AMQ2580Test.class);

    private static final String TOPIC_NAME = "topicName";
    private static final String CLIENT_ID = "client_id";
    private static final String textOfSelectedMsg = "good_message";

    protected TopicConnection connection;

    private Topic topic;
    private Session session;
    private MessageProducer producer;
    private ConnectionFactory connectionFactory;
    private TopicConnection topicConnection;
    private BrokerService service;

    public static Test suite() {
        return suite(AMQ2580Test.class);
    }

    protected void setUp() throws Exception {
        super.setUp();
        initDurableBroker();
        initConnectionFactory();
        initTopic();
    }

    protected void tearDown() throws Exception {
        shutdownClient();
        service.stop();
        super.tearDown();
    }

    private void initConnection() throws JMSException {
        if (connection == null) {
            LOG.info("Initializing connection");

            connection = (TopicConnection) connectionFactory.createConnection();
            connection.start();
        }
    }

    public void initCombosForTestTopicIsDurableSmokeTest() throws Exception {
        addCombinationValues("defaultPersistenceAdapter", PersistenceAdapterChoice.values());
    }

    public void testTopicIsDurableSmokeTest() throws Exception {

        initClient();
        MessageConsumer consumer = createMessageConsumer();
        LOG.info("Consuming message");
        assertNull(consumer.receive(1));
        shutdownClient();
        consumer.close();

        sendMessages();
        shutdownClient();

        initClient();
        consumer = createMessageConsumer();

        LOG.info("Consuming message");
        TextMessage answer1 = (TextMessage) consumer.receive(1000);
        assertNotNull("we got our message", answer1);

        consumer.close();
    }

    private MessageConsumer createMessageConsumer() throws JMSException {
        LOG.info("creating durable subscriber");
        return session.createDurableSubscriber(topic,
                TOPIC_NAME,
                "name='value'",
                false);
    }

    private void initClient() throws JMSException {
        LOG.info("Initializing client");

        initConnection();
        initSession();
    }

    private void shutdownClient()
            throws JMSException {
        LOG.info("Closing session and connection");
        session.close();
        connection.close();
        session = null;
        connection = null;
    }

    private void sendMessages()
            throws JMSException {
        initConnection();

        initSession();

        LOG.info("Creating producer");
        producer = session.createProducer(topic);

        sendMessageThatFailsSelection();

        sendMessage(textOfSelectedMsg, "value");
    }

    private void initSession() throws JMSException {
        LOG.info("Initializing session");
        session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private void sendMessageThatFailsSelection() throws JMSException {
        for (int i = 0; i < 5; i++) {
            String textOfNotSelectedMsg = "Msg_" + i;
            sendMessage(textOfNotSelectedMsg, "not_value");
            LOG.info("#");
        }
    }

    private void sendMessage(
            String msgText,
            String propertyValue) throws JMSException {
        LOG.info("Creating message: " + msgText);
        TextMessage messageToSelect = session.createTextMessage(msgText);
        messageToSelect.setStringProperty("name", propertyValue);
        LOG.info("Sending message");
        producer.send(messageToSelect);
    }

    protected void initConnectionFactory() throws Exception {
        ActiveMQConnectionFactory activeMqConnectionFactory = createActiveMqConnectionFactory();
        connectionFactory = activeMqConnectionFactory;
    }


    private ActiveMQConnectionFactory createActiveMqConnectionFactory() throws Exception {
        ActiveMQConnectionFactory activeMqConnectionFactory = new ActiveMQConnectionFactory(
                "failover:" + service.getTransportConnectors().get(0).getConnectUri().toString());
        activeMqConnectionFactory.setWatchTopicAdvisories(false);
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setDurableTopicPrefetch(2);
        prefetchPolicy.setOptimizeDurableTopicPrefetch(2);
        activeMqConnectionFactory.setPrefetchPolicy(prefetchPolicy);
        activeMqConnectionFactory.setClientID(CLIENT_ID);
        return activeMqConnectionFactory;
    }

    private void initDurableBroker() throws Exception {
        service = new BrokerService();
        setDefaultPersistenceAdapter(service);
        service.setDeleteAllMessagesOnStartup(true);
        service.setAdvisorySupport(false);
        service.setTransportConnectorURIs(new String[]{"tcp://localhost:0"});
        service.setPersistent(true);
        service.setUseJmx(false);
        service.start();

    }

    private void initTopic() throws JMSException {
        topicConnection = (TopicConnection) connectionFactory.createConnection();
        TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = topicSession.createTopic(TOPIC_NAME);
    }
}
