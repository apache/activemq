/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.config;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import org.apache.activeio.command.DefaultWireFormat;
import org.apache.activeio.command.WireFormat;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.FixedSizedSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.NoSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.broker.region.policy.StrictOrderDispatchPolicy;
import org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.TimedSubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.journal.JournalPersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.transport.activeio.ActiveIOTransportServer;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

/**
 * @version $Revision$
 */
public class ConfigTest extends TestCase {
    
    private static final Log log = LogFactory.getLog(ConfigTest.class);
    
    protected static final String JOURNAL_ROOT = "target/test-data/";
    protected static final String DERBY_ROOT = "target/test-data/";
    protected static final String CONF_ROOT = "src/test/resources/org/apache/activemq/config/sample-conf/";

    static {
        System.setProperty("javax.net.ssl.trustStore", "src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");
    }

    /*
     * IMPORTANT NOTE: Assertions checking for the existence of the derby directory will fail if the first derby
     *                 directory is not created under target/test-data/. The test in unable to change the derby
     *                 root directory for succeeding creation. It uses the first created directory as the root.
     */

    /*
     * This tests creating a journal persistence adapter using the persistence adapter factory bean
     */
    public void testJournaledJDBCConfig() throws Exception {
        System.out.print("Checking journaled JDBC persistence adapter configuration... ");

        File journalFile = new File(JOURNAL_ROOT + "testJournaledJDBCConfig/journal");
        recursiveDelete(journalFile);

        File derbyFile = new File(DERBY_ROOT + "testJournaledJDBCConfig/derbydb"); // Default derby name
        recursiveDelete(derbyFile);

        BrokerService broker;
        broker = createBroker(new FileSystemResource(CONF_ROOT + "journaledjdbc-example.xml"));
        try {
            assertEquals("Broker Config Error (brokerName)", "brokerJournaledJDBCConfigTest", broker.getBrokerName());

            PersistenceAdapter adapter = broker.getPersistenceAdapter();

            assertTrue("Should have created a journal persistence adapter", adapter instanceof JournalPersistenceAdapter);
            assertTrue("Should have created a derby directory at " + derbyFile.getAbsolutePath(), derbyFile.exists());
            assertTrue("Should have created a journal directory at " + journalFile.getAbsolutePath(), journalFile.exists());

            
            // Check persistence factory configurations
            System.out.print("Checking persistence adapter factory settings... ");
            JournalPersistenceAdapter pa = (JournalPersistenceAdapter) broker.getPersistenceAdapter();
            
            log.info("Success");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /*
     * This tests creating a jdbc persistence adapter using xbeans-spring
     */
    public void testJdbcConfig() throws Exception {
        System.out.print("Checking jdbc persistence adapter configuration... ");

        BrokerService broker;
        broker = createBroker(new FileSystemResource(CONF_ROOT + "jdbc-example.xml"));
        try {
            assertEquals("Broker Config Error (brokerName)", "brokerJdbcConfigTest", broker.getBrokerName());

            PersistenceAdapter adapter = broker.getPersistenceAdapter();

            assertTrue("Should have created a jdbc persistence adapter", adapter instanceof JDBCPersistenceAdapter);
            assertEquals("JDBC Adapter Config Error (cleanupPeriod)", 60000,
                    ((JDBCPersistenceAdapter)adapter).getCleanupPeriod());
            assertTrue("Should have created an EmbeddedDataSource",
                    ((JDBCPersistenceAdapter)adapter).getDataSource() instanceof EmbeddedDataSource);
            assertTrue("Should have created a DefaultWireFormat",
                    ((JDBCPersistenceAdapter)adapter).getWireFormat() instanceof DefaultWireFormat);

            log.info("Success");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /*
     * This tests configuring the different broker properties using xbeans-spring
     */
    public void testBrokerConfig() throws Exception {
        ActiveMQTopic dest;
        BrokerService broker;

        File journalFile = new File(JOURNAL_ROOT);
        recursiveDelete(journalFile);

        // Create broker from resource
        System.out.print("Creating broker... ");
        broker = createBroker("org/apache/activemq/config/example.xml");
        log.info("Success");

        try {
            // Check broker configuration
            System.out.print("Checking broker configurations... ");
            assertEquals("Broker Config Error (brokerName)", "brokerConfigTest", broker.getBrokerName());
            assertEquals("Broker Config Error (populateJMSXUserID)", false, broker.isPopulateJMSXUserID());
            assertEquals("Broker Config Error (useLoggingForShutdownErrors)", true, broker.isUseLoggingForShutdownErrors());
            assertEquals("Broker Config Error (useJmx)", true, broker.isUseJmx());
            assertEquals("Broker Config Error (persistent)", false, broker.isPersistent());
            assertEquals("Broker Config Error (useShutdownHook)", false, broker.isUseShutdownHook());
            assertEquals("Broker Config Error (deleteAllMessagesOnStartup)", true, broker.isDeleteAllMessagesOnStartup());
            log.info("Success");

            // Check specific vm transport
            System.out.print("Checking vm connector... ");
            assertEquals("Should have a specific VM Connector", "vm://javacoola", broker.getVmConnectorURI().toString());
            log.info("Success");

            // Check transport connectors list
            System.out.print("Checking transport connectors... ");
            List connectors = broker.getTransportConnectors();
            assertTrue("Should have created at least 4 connectors", (connectors.size() >= 4));
            assertTrue ("1st connector should be TcpTransportServer", ((TransportConnector)connectors.get(0)).getServer() instanceof TcpTransportServer);
            assertTrue ("2nd connector should be TcpTransportServer", ((TransportConnector)connectors.get(1)).getServer() instanceof TcpTransportServer);
            assertTrue ("3rd connector should be TcpTransportServer", ((TransportConnector)connectors.get(2)).getServer() instanceof TcpTransportServer);
            assertTrue ("4th connector should be ActiveIOTransportServer", ((TransportConnector)connectors.get(3)).getServer() instanceof ActiveIOTransportServer);

            // Check spring configured transport server (last transport connector only)
            ActiveIOTransportServer myTransportServer = (ActiveIOTransportServer)((TransportConnector)connectors.get(3)).getServer();
            assertEquals("URI should be ssl", "ssl://localhost:61634", myTransportServer.getConnectURI().toString());
            assertEquals("Error transport server config (stopTimeout)", 5000, myTransportServer.getStopTimeout());

            // Check spring configured wire format factory
            WireFormat myWireFormat = myTransportServer.getWireFormatFactory().createWireFormat();
            assertTrue("WireFormat should be OpenWireFormat", myWireFormat instanceof OpenWireFormat);
            assertEquals("WireFormat Config Error (stackTraceEnabled)", false, ((OpenWireFormat)myWireFormat).getPreferedWireFormatInfo().isStackTraceEnabled());
            assertEquals("WireFormat Config Error (tcpNoDelayEnabled)", true, ((OpenWireFormat)myWireFormat).getPreferedWireFormatInfo().isTcpNoDelayEnabled());
            assertEquals("WireFormat Config Error (cacheEnabled)", false, ((OpenWireFormat)myWireFormat).getPreferedWireFormatInfo().isCacheEnabled());
            log.info("Success");

            // Check network connectors
            System.out.print("Checking network connectors... ");
            List networkConnectors = broker.getNetworkConnectors();
            assertEquals("Should have a single network connector", 1, networkConnectors.size());
            log.info("Success");

            // Check dispatch policy configuration
            System.out.print("Checking dispatch policies... ");

            dest = new ActiveMQTopic("Topic.SimpleDispatch");
            assertTrue("Should have a simple dispatch policy for " + dest.getTopicName(),
                    broker.getDestinationPolicy().getEntryFor(dest).getDispatchPolicy() instanceof SimpleDispatchPolicy);

            dest = new ActiveMQTopic("Topic.RoundRobinDispatch");
            assertTrue("Should have a round robin dispatch policy for " + dest.getTopicName(),
                    broker.getDestinationPolicy().getEntryFor(dest).getDispatchPolicy() instanceof RoundRobinDispatchPolicy);

            dest = new ActiveMQTopic("Topic.StrictOrderDispatch");
            assertTrue("Should have a strict order dispatch policy for " + dest.getTopicName(),
                    broker.getDestinationPolicy().getEntryFor(dest).getDispatchPolicy() instanceof StrictOrderDispatchPolicy);
            log.info("Success");

            // Check subscription policy configuration
            System.out.print("Checking subscription recovery policies... ");
            SubscriptionRecoveryPolicy subsPolicy;

            dest = new ActiveMQTopic("Topic.FixedSizedSubs");
            subsPolicy = broker.getDestinationPolicy().getEntryFor(dest).getSubscriptionRecoveryPolicy();
            assertTrue("Should have a fixed sized subscription recovery policy for " + dest.getTopicName(),
                  subsPolicy instanceof FixedSizedSubscriptionRecoveryPolicy);
            assertEquals("FixedSizedSubsPolicy Config Error (maximumSize)", 2000000,
                  ((FixedSizedSubscriptionRecoveryPolicy)subsPolicy).getMaximumSize());
            assertEquals("FixedSizedSubsPolicy Config Error (useSharedBuffer)", false,
                  ((FixedSizedSubscriptionRecoveryPolicy)subsPolicy).isUseSharedBuffer());

            dest = new ActiveMQTopic("Topic.LastImageSubs");
            subsPolicy = broker.getDestinationPolicy().getEntryFor(dest).getSubscriptionRecoveryPolicy();
            assertTrue("Should have a last image subscription recovery policy for " + dest.getTopicName(),
                    subsPolicy instanceof LastImageSubscriptionRecoveryPolicy);

            dest = new ActiveMQTopic("Topic.NoSubs");
            subsPolicy = broker.getDestinationPolicy().getEntryFor(dest).getSubscriptionRecoveryPolicy();
            assertTrue("Should have no subscription recovery policy for " + dest.getTopicName(),
                    subsPolicy instanceof NoSubscriptionRecoveryPolicy);

            dest = new ActiveMQTopic("Topic.TimedSubs");
            subsPolicy = broker.getDestinationPolicy().getEntryFor(dest).getSubscriptionRecoveryPolicy();
            assertTrue("Should have a timed subscription recovery policy for " + dest.getTopicName(),
                    subsPolicy instanceof TimedSubscriptionRecoveryPolicy);
            assertEquals("TimedSubsPolicy Config Error (recoverDuration)", 25000,
                    ((TimedSubscriptionRecoveryPolicy)subsPolicy).getRecoverDuration());
            log.info("Success");

            // Check usage manager
            System.out.print("Checking memory manager configurations... ");
            UsageManager memMgr = broker.getMemoryManager();
            assertTrue("Should have a memory manager", memMgr != null);
            assertEquals("UsageManager Config Error (limit)", 200000, memMgr.getLimit());
            assertEquals("UsageManager Config Error (percentUsageMinDelta)", 20, memMgr.getPercentUsageMinDelta());
            log.info("Success");

            log.info("Success");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /*
     * This tests creating a journal persistence adapter using xbeans-spring
     */
    public void testJournalConfig() throws Exception {
        System.out.print("Checking journal persistence adapter configuration... ");

        File journalFile = new File(JOURNAL_ROOT + "testJournalConfig/journal");
        recursiveDelete(journalFile);

        BrokerService broker;
        broker = createBroker(new FileSystemResource(CONF_ROOT + "journal-example.xml"));
        try {
            assertEquals("Broker Config Error (brokerName)", "brokerJournalConfigTest", broker.getBrokerName());

            PersistenceAdapter adapter = broker.getPersistenceAdapter();

            assertTrue("Should have created a journal persistence adapter", adapter instanceof JournalPersistenceAdapter);
            assertTrue("Should have created a journal directory at " + journalFile.getAbsolutePath(), journalFile.exists());

            log.info("Success");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /*
     * This tests creating a memory persistence adapter using xbeans-spring
     */
    public void testMemoryConfig() throws Exception {
        System.out.print("Checking memory persistence adapter configuration... ");

        File journalFile = new File(JOURNAL_ROOT + "testMemoryConfig");
        recursiveDelete(journalFile);

        File derbyFile = new File(DERBY_ROOT + "testMemoryConfig");
        recursiveDelete(derbyFile);

        BrokerService broker;
        broker = createBroker(new FileSystemResource(CONF_ROOT + "memory-example.xml"));

        try {
            assertEquals("Broker Config Error (brokerName)", "brokerMemoryConfigTest", broker.getBrokerName());

            PersistenceAdapter adapter = broker.getPersistenceAdapter();

            assertTrue("Should have created a memory persistence adapter", adapter instanceof MemoryPersistenceAdapter);
            assertTrue("Should have not created a derby directory at " + derbyFile.getAbsolutePath(), !derbyFile.exists());
            assertTrue("Should have not created a journal directory at " + journalFile.getAbsolutePath(), !journalFile.exists());

            log.info("Success");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }

    }

    public void testXmlConfigHelper() throws Exception {
        BrokerService broker;

        broker = createBroker(new FileSystemResource(CONF_ROOT + "memory-example.xml"));
        try {
            assertEquals("Broker Config Error (brokerName)", "brokerMemoryConfigTest", broker.getBrokerName());
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }

        broker = createBroker("org/apache/activemq/config/config.xml");
        try {
            assertEquals("Broker Config Error (brokerName)", "brokerXmlConfigHelper", broker.getBrokerName());
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    /*
     * TODO: Create additional tests for forwarding bridges
     */

    protected static void recursiveDelete(File file) {
        if( file.isDirectory() ) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        file.delete();
    }

    protected BrokerService createBroker(String resource) throws Exception {
        return createBroker(new ClassPathResource(resource));
    }

    protected BrokerService createBroker(Resource resource) throws Exception {
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();

        BrokerService broker = factory.getBroker();

        assertTrue("Should have a broker!", broker != null);

        //Broker is already started by default when using the XML file
       // broker.start();

        return broker;
    }
}
