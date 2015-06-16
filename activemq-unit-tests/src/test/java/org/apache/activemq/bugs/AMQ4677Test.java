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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.*;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.leveldb.LevelDBStoreViewMBean;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4677Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4677Test.class);
    private static BrokerService brokerService;

    @Rule public TestName name = new TestName();

    private File dataDirFile;

    @Before
    public void setUp() throws Exception {

        dataDirFile = new File("target/LevelDBCleanupTest");

        brokerService = new BrokerService();
        brokerService.setBrokerName("LevelDBBroker");
        brokerService.setPersistent(true);
        brokerService.setUseJmx(true);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setDataDirectoryFile(dataDirFile);

        LevelDBStore persistenceFactory = new LevelDBStore();
        persistenceFactory.setDirectory(dataDirFile);
        brokerService.setPersistenceAdapter(persistenceFactory);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testSendAndReceiveAllMessages() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://LevelDBBroker");

        Connection connection = connectionFactory.createConnection();
        connection.setClientID(getClass().getName());
        connection.start();

        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.toString());
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        final LevelDBStoreViewMBean levelDBView = getLevelDBStoreMBean();
        assertNotNull(levelDBView);
        levelDBView.compact();

        final int SIZE = 10 * 1024;
        final int MSG_COUNT = 30000;    // very slow consuming 60k messages of size 30k
        final CountDownLatch done = new CountDownLatch(MSG_COUNT);

        byte buffer[] = new byte[SIZE];
        for (int i = 0; i < SIZE; ++i) {
            buffer[i] = (byte) 128;
        }

        for (int i = 0; i < MSG_COUNT; ++i) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(buffer);
            producer.send(message);

            if ((i % 1000) == 0) {
                LOG.info("Sent message #{}", i);
                session.commit();
            }
        }

        session.commit();

        LOG.info("Finished sending all messages.");

        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                if ((done.getCount() % 1000) == 0) {
                    try {
                        LOG.info("Received message #{}", MSG_COUNT - done.getCount());
                        session.commit();
                    } catch (JMSException e) {
                    }
                }
                done.countDown();
            }
        });

        done.await(15, TimeUnit.MINUTES);
        session.commit();
        LOG.info("Finished receiving all messages.");

        assertTrue("Should < 3 logfiles left.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                levelDBView.compact();
                return countLogFiles() < 3;
            }
        }, TimeUnit.MINUTES.toMillis(5), (int)TimeUnit.SECONDS.toMillis(30)));

        levelDBView.compact();
        LOG.info("Current number of logs {}", countLogFiles());
    }

    protected long countLogFiles() {
        String[] logFiles = dataDirFile.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if (name.endsWith("log")) {
                    return true;
                }
                return false;
            }
        });

        LOG.info("Current number of logs {}", logFiles.length);
        return logFiles.length;
    }

    protected LevelDBStoreViewMBean getLevelDBStoreMBean() throws Exception {
        ObjectName levelDbViewMBeanQuery = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=LevelDBBroker,service=PersistenceAdapter,instanceName=LevelDB*");

        Set<ObjectName> names = brokerService.getManagementContext().queryNames(null, levelDbViewMBeanQuery);
        if (names.isEmpty() || names.size() > 1) {
            throw new java.lang.IllegalStateException("Can't find levelDB store name.");
        }

        LevelDBStoreViewMBean proxy = (LevelDBStoreViewMBean) brokerService.getManagementContext()
                .newProxyInstance(names.iterator().next(), LevelDBStoreViewMBean.class, true);
        return proxy;
    }
}
