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
package org.apache.activemq.broker;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.usage.SystemUsage;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedeliveryRestartWithExceptionTest extends TestSupport {

    private static final transient Logger LOG = LoggerFactory.getLogger(RedeliveryRestartWithExceptionTest.class);
    ActiveMQConnection connection;
    BrokerService broker = null;
    String queueName = "redeliveryRestartQ";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();
        configureBroker(broker, true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        broker.stop();
        super.tearDown();
    }

    protected void configureBroker(BrokerService broker, boolean throwExceptionOnUpdate) throws Exception {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setPersistJMSRedelivered(true);
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.setPersistenceAdapter(new KahaDBWithUpdateExceptionPersistenceAdapter(throwExceptionOnUpdate));
        broker.addConnector("tcp://0.0.0.0:0");
    }

    @org.junit.Test
    public void testValidateRedeliveryFlagAfterRestart() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
            + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        populateDestination(10, destination, connection, true);
        TextMessage msg = null;
        MessageConsumer consumer = session.createConsumer(destination);
        Exception expectedException = null;
        try {
            for (int i = 0; i < 5; i++) {
                msg = (TextMessage) consumer.receive(5000);
                LOG.info("not redelivered? got: " + msg);
                assertNotNull("got the message", msg);
                assertTrue("Should not receive the 5th message", i < 4);
                //The first 4 messages will be ok but the 5th one should hit an exception in updateMessage and should not be delivered
            }
        } catch (Exception e) {
            //Expecting an exception and disconnect on the 5th message
            LOG.info("Got expected:", e);
            expectedException = e;
        }
        assertNotNull("Expecting an exception when updateMessage fails", expectedException);                
        
        consumer.close();
        safeCloseConnection(connection);
        
        restartBroker();
        
        connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
            + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);
        
        
        // consume the messages that were previously delivered
        for (int i = 0; i < 4; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("re delivery flag", true, msg.getJMSRedelivered());
            assertTrue("redelivery count survives restart", msg.getLongProperty("JMSXDeliveryCount") > 1);
            msg.acknowledge();
        }
        

        // consume the rest that were not redeliveries
        for (int i = 0; i < 6; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            msg.acknowledge();
        }
        connection.close();
    }


    @org.junit.Test
    public void testValidateRedeliveryFlagAfterTransientFailureConnectionDrop() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
            + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        populateDestination(10, destination, connection, true);
        TextMessage msg = null;
        MessageConsumer consumer = session.createConsumer(destination);
        Exception expectedException = null;
        try {
            for (int i = 0; i < 5; i++) {
                msg = (TextMessage) consumer.receive(5000);
                LOG.info("not redelivered? got: " + msg);
                assertNotNull("got the message", msg);
                assertTrue("Should not receive the 5th message", i < 4);
                //The first 4 messages will be ok but the 5th one should hit an exception in updateMessage and should not be delivered
            }
        } catch (Exception e) {
            //Expecting an exception and disconnect on the 5th message
            LOG.info("Got expected:", e);
            expectedException = e;
        }
        assertNotNull("Expecting an exception when updateMessage fails", expectedException);

        consumer.close();
        safeCloseConnection(connection);

        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);


        // consume the messages that were previously delivered
        for (int i = 0; i < 4; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("re delivery flag on:" + i, true, msg.getJMSRedelivered());
            assertTrue("redelivery count survives reconnect for:" + i, msg.getLongProperty("JMSXDeliveryCount") > 1);
            msg.acknowledge();
        }


        // consume the rest that were not redeliveries
        for (int i = 0; i < 6; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            msg.acknowledge();
        }
        connection.close();
    }

    @org.junit.Test
    public void testValidateRedeliveryFlagOnNonPersistentAfterTransientFailureConnectionDrop() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
            + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        populateDestination(10, destination, connection, false);
        TextMessage msg = null;
        MessageConsumer consumer = session.createConsumer(destination);
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(5000);
            assertNotNull("got the message", msg);
            assertFalse("not redelivered", msg.getJMSRedelivered());
        }

        connection.getTransport().narrow(TcpTransport.class).getTransportListener().onException(new IOException("Die"));

        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);

        // consume the messages that were previously delivered
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("redelivery flag set on:" + i, true, msg.getJMSRedelivered());
            assertTrue("redelivery count survives reconnect for:" + i, msg.getLongProperty("JMSXDeliveryCount") > 1);
            msg.acknowledge();
        }

        // consume the rest that were not redeliveries
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            msg.acknowledge();
        }
        connection.close();
    }

    private void restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = createRestartedBroker();
        broker.start();
    }

    private BrokerService createRestartedBroker() throws Exception {
        broker = new BrokerService();
        configureBroker(broker, false);
        return broker;
    }

    private void populateDestination(final int nbMessages, final Destination destination, javax.jms.Connection connection, boolean persistent) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }
    
    private class KahaDBWithUpdateExceptionPersistenceAdapter implements PersistenceAdapter {
        
        private KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
        private boolean throwExceptionOnUpdate;
        
        public KahaDBWithUpdateExceptionPersistenceAdapter(boolean throwExceptionOnUpdate) {
            this.throwExceptionOnUpdate = throwExceptionOnUpdate;
        }
        
        @Override
        public void start() throws Exception {
            kahaDB.start();
        }

        @Override
        public void stop() throws Exception {
            kahaDB.stop();
        }

        @Override
        public Set<ActiveMQDestination> getDestinations() {
            return kahaDB.getDestinations();
        }

        @Override
        public MessageStore createQueueMessageStore(ActiveMQQueue destination) 
                throws IOException {
            MessageStore proxyMessageStoreWithException = new ProxyMessageStoreWithUpdateException(
                    kahaDB.createQueueMessageStore(destination), throwExceptionOnUpdate);
            return proxyMessageStoreWithException;
        }

        @Override
        public TopicMessageStore createTopicMessageStore(
                ActiveMQTopic destination) throws IOException {
            TopicMessageStore proxyMessageStoreWithException = new ProxyTopicMessageStoreWithUpdateException(
                    kahaDB.createTopicMessageStore(destination), throwExceptionOnUpdate);
            return proxyMessageStoreWithException;
        }
        
        @Override
        public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
            return kahaDB.createJobSchedulerStore();
        }

        @Override
        public void removeQueueMessageStore(ActiveMQQueue destination) {
            kahaDB.removeQueueMessageStore(destination);
        }

        @Override
        public void removeTopicMessageStore(ActiveMQTopic destination) {
            kahaDB.removeTopicMessageStore(destination);
        }

        @Override
        public TransactionStore createTransactionStore() throws IOException {
            return kahaDB.createTransactionStore();
        }

        @Override
        public void beginTransaction(ConnectionContext context)
                throws IOException {
            kahaDB.beginTransaction(context);
        }

        @Override
        public void commitTransaction(ConnectionContext context)
                throws IOException {
            kahaDB.commitTransaction(context);
        }

        @Override
        public void rollbackTransaction(ConnectionContext context)
                throws IOException {
            kahaDB.rollbackTransaction(context);
        }

        @Override
        public long getLastMessageBrokerSequenceId() throws IOException {
            return kahaDB.getLastMessageBrokerSequenceId();
        }

        @Override
        public void deleteAllMessages() throws IOException {
            kahaDB.deleteAllMessages();
        }

        @Override
        public void setUsageManager(SystemUsage usageManager) {
            kahaDB.setUsageManager(usageManager);
        }

        @Override
        public void setBrokerName(String brokerName) {
            kahaDB.setBrokerName(brokerName);
        }

        @Override
        public void setDirectory(File dir) {
            kahaDB.setDirectory(dir);
        }

        @Override
        public File getDirectory() {
            return kahaDB.getDirectory();
        }

        @Override
        public void checkpoint(boolean sync) throws IOException {
            kahaDB.checkpoint(sync);
        }

        @Override
        public long size() {
            return kahaDB.size();
        }

        @Override
        public long getLastProducerSequenceId(ProducerId id) throws IOException {
            return kahaDB.getLastProducerSequenceId(id);
        }

        @Override
        public void allowIOResumption() {
            kahaDB.allowIOResumption();
        }

    }
    
    private class ProxyMessageStoreWithUpdateException extends ProxyMessageStore {
        private boolean throwExceptionOnUpdate;
        private int numBeforeException = 4;
        public ProxyMessageStoreWithUpdateException(MessageStore delegate, boolean throwExceptionOnUpdate) {
            super(delegate);
            this.throwExceptionOnUpdate = throwExceptionOnUpdate;
        }
        
        @Override
        public void updateMessage(Message message) throws IOException {
            if(throwExceptionOnUpdate) {
                if(numBeforeException > 0) {
                    numBeforeException--;
                    super.updateMessage(message);
                } else {
                    // lets only do it once so we can validate transient store failure
                    throwExceptionOnUpdate = false;

                    //A message that has never been delivered will hit this exception
                    throw new IOException("Hit our simulated exception writing the update to disk");
                }
            } else {
                super.updateMessage(message);
            }
        }
    }
    
    private class ProxyTopicMessageStoreWithUpdateException extends ProxyTopicMessageStore {
        private boolean throwExceptionOnUpdate;
        private int numBeforeException = 4;
        public ProxyTopicMessageStoreWithUpdateException(TopicMessageStore delegate, boolean throwExceptionOnUpdate) {
            super(delegate);
            this.throwExceptionOnUpdate = throwExceptionOnUpdate;
        }
        
        @Override
        public void updateMessage(Message message) throws IOException {
            if(throwExceptionOnUpdate) {
                if(numBeforeException > 0) {
                    numBeforeException--;
                    super.updateMessage(message);
                } else {
                    //A message that has never been delivered will hit this exception
                    throw new IOException("Hit our simulated exception writing the update to disk");
                }
            } else {
                super.updateMessage(message);
            }
        }
    }
}