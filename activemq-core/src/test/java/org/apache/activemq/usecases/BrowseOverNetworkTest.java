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
import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.region.QueueSubscription;
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

    public void testconsumerInfo() throws Exception {
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

    protected int browseMessages(String broker, Destination dest) throws Exception {
        QueueBrowser browser = createBrowser(broker, dest);
        Enumeration msgs = browser.getEnumeration();
        int browsedMessage = 0;
        while (msgs.hasMoreElements()) {
            browsedMessage++;
            msgs.nextElement();
        }
        return browsedMessage;
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

}
