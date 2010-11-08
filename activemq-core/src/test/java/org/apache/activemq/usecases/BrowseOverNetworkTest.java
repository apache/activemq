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
package org.apache.activemq.usecases;

import java.net.URI;
import java.util.Arrays;
import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.region.QueueSubscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.ClassPathResource;

public class BrowseOverNetworkTest extends JmsMultipleBrokersTestSupport {
    private static final Log LOG = LogFactory.getLog(QueueSubscription.class);
    protected static final int MESSAGE_COUNT = 10;

    public void testBrowse() throws Exception {
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));

        bridgeBrokers("BrokerA", "BrokerB");


        startAllBrokers();

        Destination dest = createDestination("TEST.FOO", false);

        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        browseMessages("BrokerB", dest);

        Thread.sleep(2000);

        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);

        Thread.sleep(2000);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT);

        LOG.info("A+B: " + msgsA.getMessageCount() + "+"
                + msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount()
                + msgsB.getMessageCount());
    }

    public void testConsumerInfo() throws Exception {
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker1.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker2.xml"));

        startAllBrokers();

        brokers.get("broker1").broker.waitUntilStarted();


        Destination dest = createDestination("QUEUE.A,QUEUE.B", false);


        int broker1 = browseMessages("broker1", dest);
        assertEquals("Browsed a message on an empty queue", 0, broker1);
        Thread.sleep(1000);
        int broker2 = browseMessages("broker2", dest);
        assertEquals("Browsed a message on an empty queue", 0, broker2);

    }

    public class Browser extends Thread {

        String broker;
        Destination dest;
        int totalCount;
        QueueBrowser browser = null;
        MessageConsumer consumer = null;
        boolean consume = false;

        public Browser(String broker, Destination dest) {
            this.broker = broker;
            this.dest = dest;
        }

        public void run() {
            int retries = 0;
            while (retries++ < 5) {
                try {
                    QueueBrowser browser = createBrowser(broker, dest);
                    int count  = browseMessages(browser, broker);
                    if (consume) {
                        if (count != 0) {
                            MessageConsumer consumer = createSyncConsumer(broker, dest);
                            totalCount += count;
                            for (int i = 0; i < count; i++) {
                                ActiveMQTextMessage message = (ActiveMQTextMessage)consumer.receive(1000);
                                LOG.info(broker + " consumer: " + message.getText() + " " + message.getDestination() +  " " + message.getMessageId() + " " + Arrays.toString(message.getBrokerPath()));
                                if (message == null) break;
                            }
                        }
                    } else {
                        totalCount = count;
                    }
                    LOG.info("browser '" + broker + "' browsed " + totalCount);

                    Thread.sleep(1000);
                } catch (Exception e) {
                    LOG.info("Exception browsing " + e, e);
                } finally {
                    try {
                        if (browser != null) {
                            browser.close();
                        }
                        if (consumer != null) {
                            consumer.close();
                        }
                    } catch (Exception e) {
                        LOG.info("Exception closing browser " + e, e);
                    }
                }
            }
        }

        public int getTotalCount() {
            return totalCount;
        }
    }

    protected NetworkConnector bridgeBrokersWithIncludedDestination(String localBrokerName, String remoteBrokerName, ActiveMQDestination included, ActiveMQDestination excluded) throws Exception {
        NetworkConnector nc = bridgeBrokers(localBrokerName, remoteBrokerName, false, 4, true);
        nc.addStaticallyIncludedDestination(included);
        if (excluded != null) {
            nc.addExcludedDestination(excluded);
        }
        nc.setPrefetchSize(1);
        return nc;
    }


    public void testMultipleBrowsers() throws Exception {
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false&brokerId=BrokerA"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false&brokerId=BrokerB"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=false&brokerId=BrokerC"));
        createBroker(new URI("broker:(tcp://localhost:61619)/BrokerD?persistent=false&useJmx=false&brokerId=BrokerD"));

        Destination composite = createDestination("TEST.FOO,TEST.BAR", false);
        Destination dest1 = createDestination("TEST.FOO", false);
        Destination dest2 = createDestination("TEST.BAR", false);

        bridgeBrokersWithIncludedDestination("BrokerA", "BrokerC", (ActiveMQDestination)composite, null);
        bridgeBrokersWithIncludedDestination("BrokerA", "BrokerB", (ActiveMQDestination)composite, null);
        bridgeBrokersWithIncludedDestination("BrokerA", "BrokerD", (ActiveMQDestination)composite, null);
        bridgeBrokersWithIncludedDestination("BrokerB", "BrokerA", (ActiveMQDestination)composite, null);
        bridgeBrokersWithIncludedDestination("BrokerB", "BrokerC", (ActiveMQDestination)composite, null);
        bridgeBrokersWithIncludedDestination("BrokerB", "BrokerD", (ActiveMQDestination)composite, null);
        bridgeBrokersWithIncludedDestination("BrokerC", "BrokerA", (ActiveMQDestination)dest2, (ActiveMQDestination)dest1);
        bridgeBrokersWithIncludedDestination("BrokerC", "BrokerB", (ActiveMQDestination)dest2, (ActiveMQDestination)dest1);
        bridgeBrokersWithIncludedDestination("BrokerC", "BrokerD", (ActiveMQDestination)dest2, (ActiveMQDestination)dest1);
        bridgeBrokersWithIncludedDestination("BrokerD", "BrokerA", (ActiveMQDestination)dest1, (ActiveMQDestination)dest2);
        bridgeBrokersWithIncludedDestination("BrokerD", "BrokerB", (ActiveMQDestination)dest1, (ActiveMQDestination)dest2);
        bridgeBrokersWithIncludedDestination("BrokerD", "BrokerC", (ActiveMQDestination)dest1, (ActiveMQDestination)dest2);

        startAllBrokers();

        brokers.get("BrokerA").broker.waitUntilStarted();
        brokers.get("BrokerC").broker.waitUntilStarted();
        brokers.get("BrokerD").broker.waitUntilStarted();

        Browser browser1 = new Browser("BrokerC", composite);
        browser1.start();

        Browser browser2 = new Browser("BrokerD", composite);
        browser2.start();

        sendMessages("BrokerA", composite, MESSAGE_COUNT);

        browser1.join();
        browser2.join();

        assertEquals(MESSAGE_COUNT * 2, browser1.getTotalCount() + browser2.getTotalCount() );

    }

    public void testAMQ3020() throws Exception {
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker1A.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker1B.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker2A.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker2B.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker3A.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/browse-broker3B.xml"));

        brokers.get("broker-1A").broker.waitUntilStarted();
        brokers.get("broker-2A").broker.waitUntilStarted();
        brokers.get("broker-3A").broker.waitUntilStarted();

        Destination composite = createDestination("PROD.FUSESOURCE.3.A,PROD.FUSESOURCE.3.B", false);

        Browser browser1 = new Browser("broker-3A", composite);
        browser1.start();

        Browser browser2 = new Browser("broker-3B", composite);
        browser2.start();

        sendMessages("broker-1A", composite, MESSAGE_COUNT);

        browser1.join();
        browser2.join();


        LOG.info("broker-3A browsed " + browser1.getTotalCount());
        LOG.info("broker-3B browsed " + browser2.getTotalCount());
        
        assertEquals(MESSAGE_COUNT * 2, browser1.getTotalCount() + browser2.getTotalCount() );

    }    

    protected int browseMessages(QueueBrowser browser, String name) throws Exception {
        Enumeration msgs = browser.getEnumeration();
        int browsedMessage = 0;
        while (msgs.hasMoreElements()) {
            browsedMessage++;
            ActiveMQTextMessage message = (ActiveMQTextMessage)msgs.nextElement();
            LOG.info(name + " browsed: " + message.getText() + " " + message.getDestination() +  " " + message.getMessageId() + " " + Arrays.toString(message.getBrokerPath()));
        }
        return browsedMessage;
    }


    protected int browseMessages(String broker, Destination dest) throws Exception {
        QueueBrowser browser = createBrowser(broker, dest);
        int browsedMessage = browseMessages(browser, "browser");
        browser.close();
        return browsedMessage;
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

}
