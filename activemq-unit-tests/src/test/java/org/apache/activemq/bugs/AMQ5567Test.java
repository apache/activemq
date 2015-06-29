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

import java.io.File;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerRestartTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.leveldb.LevelDBPersistenceAdapter;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5567Test extends BrokerRestartTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AMQ5567Test.class);

    private final ActiveMQQueue destination = new ActiveMQQueue("Q");
    private final String DATA_FOLDER = "./target/AMQ5567Test-data";

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    @Override
    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policy = new PolicyEntry();
        policy.setMemoryLimit(60*1024);
        return policy;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        IOHelper.delete(new File(DATA_FOLDER));
    }

    public void initCombosForTestPreparedTransactionNotDispatched() throws Exception {
        PersistenceAdapter[] persistenceAdapters = new PersistenceAdapter[]{
                new KahaDBPersistenceAdapter(),
                new LevelDBPersistenceAdapter(),
                new JDBCPersistenceAdapter()
        };
        for (PersistenceAdapter adapter : persistenceAdapters) {
            adapter.setDirectory(new File(DATA_FOLDER));
        }
        addCombinationValues("persistenceAdapter", persistenceAdapters);
    }

    public void testPreparedTransactionNotDispatched() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("Q");

        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);


        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(true);
        message.setTransactionId(txid);
        connection.send(message);

        connection.send(createPrepareTransaction(connectionInfo, txid));


        // send another non tx, will poke dispatch
        message = createMessage(producerInfo, destination);
        message.setPersistent(true);
        connection.send(message);


        // Since prepared but not committed.. only one should get delivered
        StubConnection connectionC = createConnection();
        ConnectionInfo connectionInfoC = createConnectionInfo();
        SessionInfo sessionInfoC = createSessionInfo(connectionInfoC);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfoC, destination);
        connectionC.send(connectionInfoC);
        connectionC.send(sessionInfoC);
        connectionC.send(consumerInfo);

        Message m = receiveMessage(connectionC, TimeUnit.SECONDS.toMillis(10));
        LOG.info("received: " + m);
        assertNotNull("Got message", m);
        assertNull("Got non tx message", m.getTransactionId());

        // cannot get the prepared message till commit
        assertNull(receiveMessage(connectionC));
        assertNoMessagesLeft(connectionC);

        LOG.info("commit: " + txid);
        connection.request(createCommitTransaction2Phase(connectionInfo, txid));

        m = receiveMessage(connectionC, TimeUnit.SECONDS.toMillis(10));
        LOG.info("received: " + m);
        assertNotNull("Got non null message", m);

    }

    public void initCombosForTestCursorStoreSync() throws Exception {
        PersistenceAdapter[] persistenceAdapters = new PersistenceAdapter[]{
                new KahaDBPersistenceAdapter(),
                new LevelDBPersistenceAdapter(),
                new JDBCPersistenceAdapter()
        };
        for (PersistenceAdapter adapter : persistenceAdapters) {
            adapter.setDirectory(new File(IOHelper.getDefaultDataDirectory()));
        }
        addCombinationValues("persistenceAdapter", persistenceAdapters);
    }

    public void testCursorStoreSync() throws Exception {

        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);


        XATransactionId txid = createXATransaction(sessionInfo);
        connection.send(createBeginTransaction(connectionInfo, txid));
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(true);
        message.setTransactionId(txid);
        connection.request(message);

        connection.request(createPrepareTransaction(connectionInfo, txid));

        QueueViewMBean proxy = getProxyToQueueViewMBean();
        assertTrue("cache is enabled", proxy.isCacheEnabled());

        // send another non tx, will fill cursor
        String payload = new String(new byte[10*1024]);
        for (int i=0; i<6; i++) {
            message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            ((TextMessage)message).setText(payload);
            connection.request(message);
        }

        assertTrue("cache is disabled", !proxy.isCacheEnabled());

        StubConnection connectionC = createConnection();
        ConnectionInfo connectionInfoC = createConnectionInfo();
        SessionInfo sessionInfoC = createSessionInfo(connectionInfoC);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfoC, destination);
        connectionC.send(connectionInfoC);
        connectionC.send(sessionInfoC);
        connectionC.send(consumerInfo);

        Message m = null;
        for (int i=0; i<3; i++) {
            m = receiveMessage(connectionC, TimeUnit.SECONDS.toMillis(10));
            LOG.info("received: " + m);
            assertNotNull("Got message", m);
            assertNull("Got non tx message", m.getTransactionId());
            connectionC.request(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        LOG.info("commit: " + txid);
        connection.request(createCommitTransaction2Phase(connectionInfo, txid));
        // consume the rest including the 2pc send in TX

        for (int i=0; i<4; i++) {
            m = receiveMessage(connectionC, TimeUnit.SECONDS.toMillis(10));
            LOG.info("received[" + i + "] " + m);
            assertNotNull("Got message", m);
            if (i==3 ) {
                assertNotNull("Got  tx message", m.getTransactionId());
            } else {
                assertNull("Got non tx message", m.getTransactionId());
            }
            connectionC.request(createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));
        }
    }

    private QueueViewMBean getProxyToQueueViewMBean()
            throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq"
                + ":destinationType=Queue,destinationName=" + destination.getQueueName()
                + ",type=Broker,brokerName=localhost");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName,
                        QueueViewMBean.class, true);
        return proxy;
    }

    public static Test suite() {
        return suite(AMQ5567Test.class);
    }

}
