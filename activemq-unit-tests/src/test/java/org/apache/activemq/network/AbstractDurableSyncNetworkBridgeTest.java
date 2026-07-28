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
package org.apache.activemq.network;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDurableSyncNetworkBridgeTest extends DynamicNetworkTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(
            AbstractDurableSyncNetworkBridgeTest.class);

    protected abstract void doSetUpLocalBroker(boolean deleteAllMessages, boolean startNetworkConnector, File dataDir) throws Exception;

    protected abstract void doSetUpRemoteBroker(boolean deleteAllMessages, File dataDir, int port) throws Exception;

    protected void restartLocalBroker(boolean startNetworkConnector) throws Exception {
        stopLocalBroker();
        doSetUpLocalBroker(false, startNetworkConnector, localBroker.getDataDirectoryFile());
    }

    protected void restartRemoteBroker() throws Exception {
        final int previousPort = remoteBroker.getTransportConnectors().get(0).getConnectUri().getPort();
        final File dataDir = remoteBroker.getDataDirectoryFile();
        stopRemoteBroker();
        try {
            doSetUpRemoteBroker(false, dataDir, previousPort);
        } catch (final IOException e) {
            if (e.getCause() instanceof java.net.BindException) {
                // Previous port still in TIME_WAIT — use a new ephemeral port
                doSetUpRemoteBroker(false, dataDir, 0);
                // Update the local broker's network connector to point to the new port
                updateLocalNetworkConnectorUri();
            } else {
                throw e;
            }
        }
    }

    protected void restartBroker(BrokerService broker, boolean startNetworkConnector) throws Exception {
        if (broker.getBrokerName().equals("localBroker")) {
            restartLocalBroker(startNetworkConnector);
        } else  {
            restartRemoteBroker();
        }
    }

    protected void waitForBridgeFullyStarted() throws Exception {
        waitForBridgeFullyStarted(TimeUnit.SECONDS.toMillis(15), true);
    }

    protected void waitForBridgeFullyStarted(long millis, boolean duplex) throws Exception {
        // Wait for the local bridge to be fully started (advisory consumers registered)
        assertTrue("Local bridge should be fully started", Wait.waitFor(() -> {
            if (localBroker.getNetworkConnectors().get(0).activeBridges().isEmpty()) {
                return false;
            }
            final NetworkBridge bridge = localBroker.getNetworkConnectors().get(0).activeBridges().iterator().next();
            if (bridge instanceof DemandForwardingBridgeSupport) {
                return ((DemandForwardingBridgeSupport) bridge).startedLatch.getCount() == 0;
            }
            return true;
        }, millis, 100));

        // Also wait for the duplex bridge on the remote broker to be fully started.
        // The duplex connector creates a separate DemandForwardingBridge on the remote side
        // that also needs its advisory consumers registered before it can process events.
        if (duplex) {
            assertTrue("Duplex bridge should be fully started", Wait.waitFor(() -> {
                final DemandForwardingBridge duplexBridge = findDuplexBridge(
                        remoteBroker.getTransportConnectors().get(0));
                return duplexBridge != null && duplexBridge.startedLatch.getCount() == 0;
            }, millis, 100));
        }
    }


    /**
     * When the remote broker restarts on a new ephemeral port (BindException fallback),
     * any existing network connector on the local broker still points to the old port.
     * This method stops the old connector and replaces it with one targeting the new URI.
     */
    protected void updateLocalNetworkConnectorUri() throws Exception {
        if (localBroker == null) {
            return;
        }
        final List<NetworkConnector> connectors = localBroker.getNetworkConnectors();
        if (connectors.isEmpty()) {
            return;
        }
        final NetworkConnector oldConnector = connectors.get(0);
        oldConnector.stop();
        localBroker.removeNetworkConnector(oldConnector);
        final NetworkConnector newConnector = configureLocalNetworkConnector();
        localBroker.addNetworkConnector(newConnector);
        newConnector.start();
    }

    protected abstract NetworkConnector configureLocalNetworkConnector() throws Exception;

}
