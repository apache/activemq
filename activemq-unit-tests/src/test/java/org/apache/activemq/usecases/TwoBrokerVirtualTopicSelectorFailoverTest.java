/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.VirtualDestinationSelectorCacheViewMBean;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.plugin.SubQueueSelectorCacheBrokerPlugin;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.ProducerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.network.DemandForwardingBridgeSupport;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.plugin.SubQueueSelectorCacheBroker;
import org.apache.log4j.Level;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertThat;

/**
 * Demonstrate issue with selector aware virtual topic consumers and failover transport.
 * <p>
 * When failover kicks in, the selectors are not applied on brokers. This happens even if {@link SubQueueSelectorCacheBroker}
 * is changed to ignore subscription consumers. The comments describe situation before fix of AMQ-6029 is applied.
 * @author <a href="https://github.com/pdudits">Patrik Dudits</a>
 */
public class TwoBrokerVirtualTopicSelectorFailoverTest extends
        JmsMultipleBrokersTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TwoBrokerVirtualTopicSelectorFailoverTest.class);

    private static final String PERSIST_SELECTOR_CACHE_FILE_BASEPATH = "./target/selectorCache-";
    private BrokerItem failover;
    private final Map<String, MessageConsumer> perSelector = new HashMap<>();

    public void testLeaksAtFailover() throws Exception {
        clearSelectorCacheFiles();
        startAllBrokers();
        NetworkConnector ncAB = bridgeBrokersWithFastReconnect("BrokerA", "BrokerB");
        ncAB.start();
        NetworkConnector ncBA = bridgeBrokersWithFastReconnect("BrokerB", "BrokerA");
        ncBA.start();
        waitForBridgeFormation();

        org.apache.log4j.Logger.getLogger(SubQueueSelectorCacheBroker.class).setLevel(Level.DEBUG);
        org.apache.log4j.Logger.getLogger(DemandForwardingBridgeSupport.class).setLevel(Level.TRACE);

        /*
         * We'll have two consumers on virtual topic listening to SYMBOL=AAPL and VIX respectively
         */
        createConsumer("AAPL");
        createConsumer("VIX");

        /*
         * Producer will produce messages onto virtual topic with symbol equal to AAPL, VIX or null
         */
        ProducerThreadTester producer = createFailoverProducer(createDestination("VirtualTopic.tempTopic", true));
        producer.addMessageProperty(null);
        producer.addMessageProperty("AAPL");
        producer.addMessageProperty("VIX");

        producer.setRunIndefinitely(true);
        producer.start();

        Thread.sleep(2000);
        LOG.warn("Shutting down broker A");
        final BrokerService brokerA = brokers.get("BrokerA").broker;
        brokerA.stop();
        
        /*
         * As we shut down, the consumers will reconnect to broker B. So far so good.
         */
        LOG.warn("Broker A shut down");
        Thread.sleep(2000);
        LOG.warn("Broker A restarting");
        
        restartBrokerA();
        LOG.warn("Broker A restarted");
        
        /*
         * As broker A restarts, consumers reconnect to it because of priority backup, and broker A registers a demand
         * at broker B. The demand is without selector, and that's the point where all the messages will get forwarded
         * to the consumer queues.
         */
        Set<String> aaplSelectorCache = getVirtualDestinationSelectorCacheMBean(brokers.get("BrokerA").broker).selectorsForDestination("queue://" + consumerQueueName("AAPL"));
        waitForBridgeFormation();

        LOG.warn("Shutting down producer");
        producer.setRunning(false);
        Thread.sleep(5000);
        LOG.warn("Verifying");

        assertEquals("SYMBOL = 'AAPL'", aaplSelectorCache.iterator().next());
        
        // Just the verification will make the consumer queues match all the messages
        Map<String,Integer> mismatch1 = verifyMismatches(producer);
        
        /*
         * The messages are forwarded even after the situation stabilizes.
         */
        LOG.info("Another batch");
        producer.setRunIndefinitely(false);
        producer.setMessageCount(200);
        producer.run();
        
        Map<String,Integer> mismatch2 = verifyMismatches(producer);
        for ( Map.Entry<String, Integer> object : mismatch2.entrySet()) {
            assertThat("Mismatch count should not rise for "+object.getKey(), object.getValue(), Matchers.lessThanOrEqualTo(mismatch1.get(object.getKey())));
        }
    }

    private Map<String, Integer> verifyMismatches(ProducerThreadTester producer) throws Exception {
        Map<String,Integer> mismatch = new HashMap<>();
        for (Map.Entry<String, AtomicInteger> c : producer.selectorCounts.entrySet()) {
            final String symbol = c.getKey();
            if (symbol == null) {
                continue;
            }
            boolean queueEmpty = true;
            int mismatches = 0;
            for (Enumeration<Message> e = brokers.get("BrokerA").createBrowser(consumerQueue(symbol)).getEnumeration();
                    e.hasMoreElements();) {
                queueEmpty = false;
                Message m = e.nextElement();
                if (!symbol.equals(m.getStringProperty("SYMBOL"))) {
                    LOG.error("Message {} doesn't match {} is {}",m.getJMSMessageID(),symbol, m.getStringProperty("SYMBOL"));
                }
                mismatches++;
            }
            // we just log those, because they fail as assertions
            LOG.error("Total {} mismatches for {}", mismatches, symbol);
            LOG.error("Producer sent {} messages matching {}, whereas {} messages were received", c.getValue().get(), symbol,
                    failover.getConsumerMessages(perSelector.get(symbol)).getMessageCount());
            mismatch.put(symbol, mismatches);
        }
        return mismatch;
    }
    
    private NetworkConnector bridgeBrokersWithFastReconnect(String local, String remote) throws Exception {
        DiscoveryNetworkConnector nc = (DiscoveryNetworkConnector) bridgeBrokers(local, remote);
        nc.setUri(URI.create(nc.getUri() + "?initialReconnectDelay=100&useExponentialBackOff=false"));
        return nc;
    }

    private MessageConsumer createConsumer(String symbol) throws JMSException, Exception {
        ActiveMQDestination consumerQueue1 = consumerQueue(symbol);
        // create it so that the queue is there and messages don't get lost
        MessageConsumer cons = failover.createConsumer(consumerQueue1, "SYMBOL = '" + symbol + "'");
        perSelector.put(symbol, cons);
        return cons;
    }

    private ActiveMQQueue consumerQueue(String symbol) throws JMSException {
        return (ActiveMQQueue) createDestination(consumerQueueName(symbol), false);
    }

    private static String consumerQueueName(String symbol) {
        return "Consumer." + symbol + ".VirtualTopic.tempTopic";
    }

    private ProducerThreadTester createFailoverProducer(javax.jms.Destination destination) throws JMSException, Exception {
        Connection conn = failover.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ProducerThreadTester rc = new ProducerThreadTester(sess, destination);
        return rc;
    }

    private String createFailoverUrl() {
        StringBuilder sb = new StringBuilder("failover:(");
        for (BrokerItem b : brokers.values()) {
            try {
                sb.append(b.broker.getTransportConnectors().get(0).getPublishableConnectString()).append(",");
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        sb.setLength(sb.length() - 1);
        sb.append(")").append("?randomize=false&priorityBackup=true&jms.watchTopicAdvisories=false&useExponentialBackOff=false");
        return sb.toString();
    }

    public VirtualDestinationSelectorCacheViewMBean getVirtualDestinationSelectorCacheMBean(BrokerService broker)
            throws MalformedObjectNameException {
        ObjectName objectName = BrokerMBeanSupport
                .createVirtualDestinationSelectorCacheName(broker.getBrokerObjectName(), "plugin", "virtualDestinationCache");
        return (VirtualDestinationSelectorCacheViewMBean) broker.getManagementContext()
                .newProxyInstance(objectName, VirtualDestinationSelectorCacheViewMBean.class, true);
    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = "?useJmx=false&deleteAllMessagesOnStartup=true";
        createAndConfigureBroker(new URI(
                "broker:(tcp://localhost:61616)/BrokerA" + options));
        createAndConfigureBroker(new URI(
                "broker:(tcp://localhost:61617)/BrokerB" + options));
    }

    @Override
    protected void startAllBrokers() throws Exception {
        super.startAllBrokers();

        ActiveMQConnectionFactory failoverConFactory = new ActiveMQConnectionFactory(createFailoverUrl());
        // brokerItem gets us lots of goodies, and we need all of them except for broker service, so we just fake it.
        failover = new BrokerItem(brokers.get("BrokerA").broker) {
            @Override
            public String getConnectionUri() {
                return createFailoverUrl();
            }

            @Override
            public void destroy() throws Exception {
                try {
                    super.destroy();
                } catch (NullPointerException npe) {
                    consumers.clear();
                    broker = null;
                    connections = null;
                    consumers = null;
                    factory = null;
                }
            }

        };
        
        failover.broker = null;
        failover.factory = failoverConFactory;
    }

    private void clearSelectorCacheFiles() {
        String[] brokerNames = new String[]{"BrokerA", "BrokerB"};
        for (String brokerName : brokerNames) {
            deleteSelectorCacheFile(brokerName);
        }
    }

    private void deleteSelectorCacheFile(String brokerName) {
        File brokerPersisteFile = new File(PERSIST_SELECTOR_CACHE_FILE_BASEPATH + brokerName);

        if (brokerPersisteFile.exists()) {
            brokerPersisteFile.delete();
        }
    }

    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);
        broker.setUseJmx(true);
        // Make topics "selectorAware"
        VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setSelectorAware(true);
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        configurePersistenceAdapter(broker);

        SubQueueSelectorCacheBrokerPlugin selectorCacheBrokerPlugin = new SubQueueSelectorCacheBrokerPlugin();
        selectorCacheBrokerPlugin.setSingleSelectorPerDestination(true);
        File persisteFile = new File(PERSIST_SELECTOR_CACHE_FILE_BASEPATH + broker.getBrokerName());
        selectorCacheBrokerPlugin.setPersistFile(persisteFile);
        broker.setPlugins(new BrokerPlugin[]{selectorCacheBrokerPlugin, new BrokerPlugin() {
            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new LogConsumers(broker);
            }

        }});
        return broker;
    }

    protected void configurePersistenceAdapter(BrokerService broker)
            throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/"
                + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }

    private void restartBrokerA() throws Exception {
        BrokerService broker = createAndConfigureBroker(new URI(
                "broker:(tcp://localhost:61616)/BrokerA"));
        broker.start(true); // force restart - not recommended
        broker.start();
        broker.waitUntilStarted();
        NetworkConnector ncAB = bridgeBrokersWithFastReconnect("BrokerA", "BrokerB");
        ncAB.start();
    }

    static class LogConsumers extends BrokerFilter {

        public LogConsumers(Broker next) {
            super(next);
        }

        @Override
        public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
            logProducer(context, info, "Remove");
            super.removeProducer(context, info);
        }

        @Override
        public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            LOG.info("Remove consumer {} at {} matching {}", info.getConsumerId(), destinationName(info.getDestination()), info.getSelector());
            super.removeConsumer(context, info); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
            logProducer(context, info, "Add");
            super.addProducer(context, info); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            LOG.info("Add consumer {} at {} matching {}", info.getConsumerId(), destinationName(info.getDestination()), info.getSelector());
            return super.addConsumer(context, info); //To change body of generated methods, choose Tools | Templates.
        }

        private void logProducer(ConnectionContext context, ProducerInfo info, String action) {
            LOG.info("{} producer {} at {}", action, info.getProducerId(), destinationName(info.getDestination()));
        }

        private static String destinationName(ActiveMQDestination dest) {
            return dest != null ? dest.getQualifiedName() : "(none)";
        }

    }

    class ProducerThreadTester extends ProducerThread {

        private Set<String> selectors = new LinkedHashSet<String>();
        private Map<String, AtomicInteger> selectorCounts = new HashMap<String, AtomicInteger>();
        private Random rand = new Random(System.currentTimeMillis());

        public ProducerThreadTester(Session session, javax.jms.Destination destination) {
            super(session, destination);
        }

        @Override
        protected Message createMessage(int i) throws Exception {
            TextMessage msg = createTextMessage(this.session, "Message-" + i);
            if (selectors.size() > 0) {
                String value = getRandomKey();
                msg.setStringProperty("SYMBOL", value);
                AtomicInteger currentCount = selectorCounts.get(value);
                currentCount.incrementAndGet();
            }

            return msg;
        }

        @Override
        public void resetCounters() {
            super.resetCounters();
            for (String key : selectorCounts.keySet()) {
                selectorCounts.put(key, new AtomicInteger(0));
            }
        }

        private String getRandomKey() {
            ArrayList<String> keys = new ArrayList(selectors);
            return keys.get(rand.nextInt(keys.size()));
        }

        public void addMessageProperty(String value) {
            if (!this.selectors.contains(value)) {
                selectors.add(value);
                selectorCounts.put(value, new AtomicInteger(0));
            }
        }

        public int getCountForProperty(String key) {
            return selectorCounts.get(key).get();
        }

    }

}
