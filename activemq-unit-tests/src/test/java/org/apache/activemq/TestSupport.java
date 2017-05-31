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
package org.apache.activemq;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.leveldb.LevelDBPersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.util.JMXSupport;

/**
 * Useful base class for unit test cases
 *
 *
 */
public abstract class TestSupport extends CombinationTestSupport {

    protected ActiveMQConnectionFactory connectionFactory;
    protected boolean topic = true;
    public PersistenceAdapterChoice defaultPersistenceAdapter = PersistenceAdapterChoice.KahaDB;

    protected ActiveMQMessage createMessage() {
        return new ActiveMQMessage();
    }

    protected Destination createDestination(String subject) {
        if (topic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }

    protected Destination createDestination() {
        return createDestination(getDestinationString());
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }

    /**
     * @param messsage
     * @param firstSet
     * @param secondSet
     */
    protected void assertTextMessagesEqual(String messsage, Message[] firstSet, Message[] secondSet)
        throws JMSException {
        assertEquals("Message count does not match: " + messsage, firstSet.length, secondSet.length);
        for (int i = 0; i < secondSet.length; i++) {
            TextMessage m1 = (TextMessage)firstSet[i];
            TextMessage m2 = (TextMessage)secondSet[i];
            assertFalse("Message " + (i + 1) + " did not match : " + messsage + ": expected {" + m1
                        + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);
            assertEquals("Message " + (i + 1) + " did not match: " + messsage + ": expected {" + m1
                         + "}, but was {" + m2 + "}", m1.getText(), m2.getText());
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }

    /**
     * Factory method to create a new connection
     */
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

    protected String getConsumerSubject() {
        return getSubject();
    }

    protected String getProducerSubject() {
        return getSubject();
    }

    protected String getSubject() {
        return getName();
    }

    public static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        f.delete();
    }

    public static void removeMessageStore() {
        if (System.getProperty("activemq.store.dir") != null) {
            recursiveDelete(new File(System.getProperty("activemq.store.dir")));
        }
        if (System.getProperty("derby.system.home") != null) {
            recursiveDelete(new File(System.getProperty("derby.system.home")));
        }
    }

    public static DestinationStatistics getDestinationStatistics(BrokerService broker, ActiveMQDestination destination) {
        DestinationStatistics result = null;
        org.apache.activemq.broker.region.Destination dest = getDestination(broker, destination);
        if (dest != null) {
            result = dest.getDestinationStatistics();
        }
        return result;
    }

    public static List<Subscription> getDestinationConsumers(BrokerService broker, ActiveMQDestination destination) {
        List<Subscription> result = null;
        org.apache.activemq.broker.region.Destination dest = getDestination(broker, destination);
        if (dest != null) {
            result = dest.getConsumers();
        }
        return result;
    }

    public static org.apache.activemq.broker.region.Destination getDestination(BrokerService target, ActiveMQDestination destination) {
        org.apache.activemq.broker.region.Destination result = null;
        for (org.apache.activemq.broker.region.Destination dest : getDestinationMap(target, destination).values()) {
            if (dest.getName().equals(destination.getPhysicalName())) {
                result = dest;
                break;
            }
        }
        return result;
    }

    private static Map<ActiveMQDestination, org.apache.activemq.broker.region.Destination> getDestinationMap(BrokerService target,
            ActiveMQDestination destination) {
        RegionBroker regionBroker = (RegionBroker) target.getRegionBroker();
        if (destination.isTemporary()) {
            return destination.isQueue() ? regionBroker.getTempQueueRegion().getDestinationMap() :
                    regionBroker.getTempTopicRegion().getDestinationMap();
        }
        return destination.isQueue() ?
                    regionBroker.getQueueRegion().getDestinationMap() :
                        regionBroker.getTopicRegion().getDestinationMap();
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        BrokerService brokerService = BrokerRegistry.getInstance().lookup("localhost");
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+ JMXSupport.encodeObjectNamePart(name));
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    public static enum PersistenceAdapterChoice {LevelDB, KahaDB, JDBC, MEM };

    public PersistenceAdapter setDefaultPersistenceAdapter(BrokerService broker) throws IOException {
        return setPersistenceAdapter(broker, defaultPersistenceAdapter);
    }

    public static PersistenceAdapter setPersistenceAdapter(BrokerService broker, PersistenceAdapterChoice choice) throws IOException {
        PersistenceAdapter adapter = null;
        switch (choice) {
        case JDBC:
            JDBCPersistenceAdapter jdbcPersistenceAdapter = new JDBCPersistenceAdapter();
            jdbcPersistenceAdapter.setUseLock(false); // rollback (at shutdown) on derby can take a long time with file io etc
            adapter = jdbcPersistenceAdapter;
            break;
        case KahaDB:
            adapter = new KahaDBPersistenceAdapter();
            break;
        case LevelDB:
            adapter = new LevelDBPersistenceAdapter();
            break;
        case MEM:
            adapter = new MemoryPersistenceAdapter();
            break;
        }
        broker.setPersistenceAdapter(adapter);
        adapter.setDirectory(new File(broker.getBrokerDataDirectory(), choice.name()));
        return adapter;
    }

    public void stopBrokerWithStoreFailure(BrokerService broker, PersistenceAdapterChoice choice) throws Exception {
        switch (choice) {
            case KahaDB:
                KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();

                // have the broker stop with an IOException on next checkpoint so it has a pending local transaction to recover
                kahaDBPersistenceAdapter.getStore().getJournal().close();
                break;
            default:
                // just stop normally by default
                broker.stop();
        }
        broker.waitUntilStopped();
    }


    /**
     * Test if base directory contains spaces
     */
    protected void assertBaseDirectoryContainsSpaces() {
        assertFalse("Base directory cannot contain spaces.", new File(System.getProperty("basedir", ".")).getAbsoluteFile().toString().contains(" "));
    }

    public void safeCloseConnection(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (JMSException ignored) {}
        }
    }
}
