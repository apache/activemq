/*
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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.broker.util.RedeliveryPlugin;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.jms.*;
import java.io.File;
import java.util.List;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.*;

public class AMQ7002Test {
    static final Logger LOG = LoggerFactory.getLogger(AMQ7002Test.class);
    protected ActiveMQConnection connection;
    protected ActiveMQConnectionFactory connectionFactory;
    private BrokerService brokerService;
    private JobSchedulerStoreImpl store;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }
    protected Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection();
    }
    public ActiveMQConnectionFactory getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            assertTrue("Should have created a connection factory!", connectionFactory != null);
        }
        return connectionFactory;
    }
    protected BrokerService createBroker() throws Exception {
        File directory = new File("target/test/ScheduledJobsDB");
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        createSchedulerStore(directory);

        BrokerService service = new BrokerService();
        service.setPersistent(true);
        service.setUseJmx(false);
        service.setJobSchedulerStore(store);
        service.setSchedulerSupport(true);
        service.setDeleteAllMessagesOnStartup(true);
        RedeliveryPlugin redeliveryPlugin = new RedeliveryPlugin();
        RedeliveryPolicy brokerRedeliveryPolicy = new RedeliveryPolicy();
        brokerRedeliveryPolicy.setInitialRedeliveryDelay(60000);
        brokerRedeliveryPolicy.setMaximumRedeliveries(20);
        brokerRedeliveryPolicy.setMaximumRedeliveryDelay(300000);
        brokerRedeliveryPolicy.setBackOffMultiplier(2);
        RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
        redeliveryPolicyMap.setDefaultEntry(brokerRedeliveryPolicy);
        redeliveryPlugin.setRedeliveryPolicyMap(redeliveryPolicyMap);
        service.setPlugins(new BrokerPlugin[]{redeliveryPlugin});
        service.start();
        service.waitUntilStarted();
        return service;
    }
    protected ConsumerObject getConsumer(int id) throws Exception {
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setInitialRedeliveryDelay(0);
        redeliveryPolicy.setMaximumRedeliveries(0);
        ActiveMQConnection consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.setRedeliveryPolicy(redeliveryPolicy);
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createQueue("Consumer." + id + ".VirtualTopic.Orders"));
        LOG.info(consumer.toString());
        ConsumerObject co = new ConsumerObject(consumerSession, consumer, consumerConnection);
        return co;
    }
    @Before
    public void before() throws Exception {
        brokerService = createBroker();
    }
    @After
    public void after() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }
    @Test
    public void TestDuplicateJobIDs() throws Exception {
        ConsumerObject consumer1 = getConsumer(1);
        ConsumerObject consumer2 = getConsumer(2);
        ActiveMQConnection producerConnection = (ActiveMQConnection) createConnection();
        producerConnection.start();
        //Session session = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Session session = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination dest = session.createTopic("VirtualTopic.Orders");
        MessageProducer producer = session.createProducer(dest);
        TextMessage msg = session.createTextMessage("Test Me");
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(msg);
        Message message1 = consumer1.getConsumer().receive();
        assertNotNull("got message", message1);
        LOG.info("got: " + message1);
        Message message2 = consumer2.getConsumer().receive();
        assertNotNull("got message", message2);
        LOG.info("got: " + message2);
        //Force rollback
        consumer1.getSession().rollback();
        consumer2.getSession().rollback();
        // Check the scheduled jobs here //
        Thread.sleep(2000);
        JobScheduler js = brokerService.getJobSchedulerStore().getJobScheduler("JMS");
        List<Job> jobList = js.getAllJobs();
        assertNotNull(jobList);
        assertEquals(2, jobList.size());
        String jobId1 = jobList.get(0).getJobId();
        String jobId2 = jobList.get(1).getJobId();
        assertFalse("FAIL: JobIDs are duplicates!",jobId1.equals(jobId2));
    }
    private class ConsumerObject {
        Session session;
        MessageConsumer consumer;
        Connection connection;
        public ConsumerObject(Session session, MessageConsumer consumer, Connection connection) {
            this.session = session;
            this.consumer = consumer;
            this.connection = connection;
        }
        public Session getSession() {
            return session;
        }
        public void setSession(Session session) {
            this.session = session;
        }
        public MessageConsumer getConsumer() {
            return consumer;
        }
        public void setConsumer(MessageConsumer consumer) {
            this.consumer = consumer;
        }
        public Connection getConnection() {
            return connection;
        }
        public void setConnection(Connection connection) {
            this.connection = connection;
        }
    }

    protected void createSchedulerStore(File directory) throws Exception {
        store = new JobSchedulerStoreImpl();
        store.setDirectory(directory);
        store.setCheckpointInterval(5000);
        store.setCleanupInterval(10000);
        //store.setJournalMaxFileLength(10 * 1024);
    }
}
