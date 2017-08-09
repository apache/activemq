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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.MessageIdList;

public class SingleBrokerVirtualDestinationsWithWildcardTest extends JmsMultipleBrokersTestSupport {

    /**
     * virtual destinations
     */
    public void testVirtualDestinations() throws Exception {
        startAllBrokers();

        sendReceive("local.test", true, "Consumer.a.local.test", false, 1, 1);
        sendReceive("global.test", true, "Consumer.a.global.test", false, 1, 1);

        destroyAllBrokers();
    }

    /**
     * virtual destinations with wild-card subscriptions (without individual virtual queue)
     */
    public void testVirtualDestinationsWithWildcardWithoutIndividualVirtualQueue() throws Exception {
        startAllBrokers();

        sendReceive("local.test.1", true, "Consumer.a.local.test.>", false, 1, 1);
        sendReceive("global.test.1", true, "Consumer.a.global.test.>", false, 1, 1);

        destroyAllBrokers();
    }

    /**
     * virtual destinations with wild-card subscriptions (with individual virtual queue)
     */
    public void testVirtualDestinationsWithWildcardWithIndividualVirtualQueue() throws Exception {
        startAllBrokers();

        sendReceive("local.test.1", true, "Consumer.a.local.test.1", false, 1, 1);
        sendReceive("local.test.1", true, "Consumer.a.local.test.>", false, 1, 2);
        sendReceive("local.test.1.2", true, "Consumer.a.local.test.>", false, 1, 1);
        sendReceive("global.test.1", true, "Consumer.a.global.test.1", false, 1, 1);
        sendReceive("global.test.1", true, "Consumer.a.global.test.>", false, 1, 2);
        sendReceive("global.test.1.2", true, "Consumer.a.global.test.>", false, 1, 1);

        destroyAllBrokers();
    }

    /**
     * virtual destinations with wild-card subscriptions (wit virtual queue pre-created)
     */
    public void testVirtualDestinationsWithWildcardWithVirtualQueuePreCreated() throws Exception {
        startAllBrokers();

        sendReceive("Consumer.a.local.test.>", false, "Consumer.a.local.test.>", false, 1, 1);
        sendReceive("local.test.1", true, "Consumer.a.local.test.>", false, 1, 1);
        sendReceive("Consumer.a.global.test.>", false, "Consumer.a.global.test.>", false, 1, 1);
        sendReceive("global.test.1", true, "Consumer.a.global.test.>", false, 1, 1);

        destroyAllBrokers();
    }

    public void sendReceive(String dest1, boolean topic1, String dest2, boolean topic2, int send, int expected) throws Exception{
        MessageConsumer client = createConsumer("BrokerA", createDestination(dest2, topic2));
        Thread.sleep(1000);
        sendMessages("BrokerA", createDestination(dest1, topic1), send);
        MessageIdList msgs = getConsumerMessages("BrokerA", client);
        msgs.setMaximumDuration(1000);
        assertEquals(expected, msgs.getMessageCount());
        client.close();
        Thread.sleep(500);
    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?useJmx=false&deleteAllMessagesOnStartup=true");
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
    }

    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);

        configurePersistenceAdapter(broker);

        // make all topics virtual and consumers use the default prefix
        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(new VirtualDestination[]{new VirtualTopic()});
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[]{virtualDestinationInterceptor};
        broker.setDestinationInterceptors(destinationInterceptors);
        return broker;
    }

    protected void configurePersistenceAdapter(BrokerService broker) throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/" + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }
}