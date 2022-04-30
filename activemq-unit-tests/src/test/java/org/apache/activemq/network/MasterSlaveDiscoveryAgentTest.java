package org.apache.activemq.network;

public class MasterSlaveDiscoveryAgentTest extends NetworkConnectionsTest {
    @Override
    protected NetworkConnector addNetworkConnector() throws Exception {
        return localBroker.addNetworkConnector("masterslave:(" + REMOTE_BROKER_TRANSPORT_URI + "," + REMOTE_BROKER_TRANSPORT_URI + ")");
    }
}
