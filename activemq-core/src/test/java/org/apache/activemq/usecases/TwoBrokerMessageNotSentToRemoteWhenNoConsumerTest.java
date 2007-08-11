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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.Command;
import org.apache.activemq.network.DemandForwardingBridge;
import org.apache.activemq.network.NetworkBridgeConfiguration;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.MessageIdList;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class TwoBrokerMessageNotSentToRemoteWhenNoConsumerTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 10;

    protected List<DemandForwardingBridge> bridges;
    protected AtomicInteger msgDispatchCount;

    /**
     * BrokerA -> BrokerB
     */
    public void testRemoteBrokerHasConsumer() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT);

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsB.getMessageCount());

        // Check that 10 message dispatch commands are send over the network
        assertEquals(MESSAGE_COUNT, msgDispatchCount.get());
    }

    /**
     * BrokerA -> BrokerB
     */
    public void testRemoteBrokerHasNoConsumer() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());

        // Check that no message dispatch commands are send over the network
        assertEquals(0, msgDispatchCount.get());
    }

    protected void bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker) throws Exception {
        List remoteTransports = remoteBroker.getTransportConnectors();
        List localTransports = localBroker.getTransportConnectors();

        URI remoteURI;
        URI localURI;
        if (!remoteTransports.isEmpty() && !localTransports.isEmpty()) {
            remoteURI = ((TransportConnector)remoteTransports.get(0)).getConnectUri();
            localURI = ((TransportConnector)localTransports.get(0)).getConnectUri();

            // Ensure that we are connecting using tcp
            if (remoteURI.toString().startsWith("tcp:") && localURI.toString().startsWith("tcp:")) {
                NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
                config.setBrokerName(localBroker.getBrokerName());
                DemandForwardingBridge bridge = new DemandForwardingBridge(config, TransportFactory.connect(localURI), TransportFactory.connect(remoteURI)) {
                    protected void serviceLocalCommand(Command command) {
                        if (command.isMessageDispatch()) {
                            // Keep track of the number of message dispatches
                            // through the bridge
                            msgDispatchCount.incrementAndGet();
                        }

                        super.serviceLocalCommand(command);
                    }
                };
                bridges.add(bridge);

                bridge.start();
            } else {
                throw new Exception("Remote broker or local broker is not using tcp connectors");
            }
        } else {
            throw new Exception("Remote broker or local broker has no registered connectors.");
        }

        maxSetupTime = 2000;
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));

        bridges = new ArrayList<DemandForwardingBridge>();
        msgDispatchCount = new AtomicInteger(0);
    }
}
