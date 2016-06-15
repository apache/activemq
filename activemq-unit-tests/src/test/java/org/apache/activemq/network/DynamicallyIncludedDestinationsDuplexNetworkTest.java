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
package org.apache.activemq.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.MessageProducer;
import javax.jms.TemporaryQueue;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.junit.Test;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class DynamicallyIncludedDestinationsDuplexNetworkTest extends SimpleNetworkTest {

    private static final int REMOTE_BROKER_TCP_PORT = 61617;

    @Override
    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/duplexDynamicIncludedDestLocalBroker.xml";
    }

    @Override
    protected BrokerService createRemoteBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("remoteBroker");
        broker.addConnector("tcp://localhost:" + REMOTE_BROKER_TCP_PORT);
        return broker;
    }

    // we have to override this, because with dynamicallyIncludedDestinations working properly
    // (see https://issues.apache.org/jira/browse/AMQ-4209) you can't get request/response
    // with temps working (there is no wild card like there is for staticallyIncludedDest)
    //
    @Override
    public void testRequestReply() throws Exception {

    }

    @Test
    public void testTempQueues() throws Exception {
        TemporaryQueue temp = localSession.createTemporaryQueue();
        MessageProducer producer = localSession.createProducer(temp);
        producer.send(localSession.createTextMessage("test"));
        Thread.sleep(100);
        assertEquals("Destination not created", 1, remoteBroker.getAdminView().getTemporaryQueues().length);
        temp.delete();
        Thread.sleep(100);
        assertEquals("Destination not deleted", 0, remoteBroker.getAdminView().getTemporaryQueues().length);
    }

    @Test
    public void testDynamicallyIncludedDestinationsForDuplex()  throws Exception{
        // Once the bridge is set up, we should see the filter used for the duplex end of the bridge
        // only subscribe to the specific destinations included in the <dynamicallyIncludedDestinations> list
        // so let's test that the filter is correct, let's also test the subscription on the localbroker
        // is correct

        // the bridge on the remote broker has the correct filter
        TransportConnection bridgeConnection = getDuplexBridgeConnectionFromRemote();
        assertNotNull(bridgeConnection);
        DemandForwardingBridge duplexBridge = getDuplexBridgeFromConnection(bridgeConnection);
        assertNotNull(duplexBridge);
        NetworkBridgeConfiguration configuration = getConfigurationFromNetworkBridge(duplexBridge);
        assertNotNull(configuration);
        assertFalse("This destinationFilter does not include ONLY the destinations specified in dynamicallyIncludedDestinations",
                configuration.getDestinationFilter().equals(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX + ">"));
        assertEquals("There are other patterns in the destinationFilter that shouldn't be there",
                "ActiveMQ.Advisory.Consumer.Queue.include.test.foo,ActiveMQ.Advisory.Consumer.Topic.include.test.bar",
                configuration.getDestinationFilter());
    }

    private NetworkBridgeConfiguration getConfigurationFromNetworkBridge(DemandForwardingBridgeSupport duplexBridge) throws NoSuchFieldException, IllegalAccessException {
        Field f = DemandForwardingBridgeSupport.class.getDeclaredField("configuration");
        f.setAccessible(true);
        NetworkBridgeConfiguration configuration = (NetworkBridgeConfiguration) f.get(duplexBridge);
        return configuration;
    }

    private DemandForwardingBridge getDuplexBridgeFromConnection(TransportConnection bridgeConnection) throws NoSuchFieldException, IllegalAccessException {
        Field f = TransportConnection.class.getDeclaredField("duplexBridge");
        f.setAccessible(true);
        DemandForwardingBridge bridge = (DemandForwardingBridge) f.get(bridgeConnection);
        return bridge;
    }

    public TransportConnection getDuplexBridgeConnectionFromRemote() {
        TransportConnector transportConnector = remoteBroker.getTransportConnectorByScheme("tcp");
        CopyOnWriteArrayList<TransportConnection> transportConnections = transportConnector.getConnections();
        TransportConnection duplexBridgeConnectionFromRemote = transportConnections.get(0);
        return duplexBridgeConnectionFromRemote;
    }

    @Override
    protected void assertNetworkBridgeStatistics(final long expectedLocalSent, final long expectedRemoteSent) throws Exception {

        final NetworkBridge localBridge = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return expectedLocalSent == localBridge.getNetworkBridgeStatistics().getDequeues().getCount() &&
                        expectedRemoteSent == localBridge.getNetworkBridgeStatistics().getReceivedCount().getCount();
            }
        }));

    }
}
