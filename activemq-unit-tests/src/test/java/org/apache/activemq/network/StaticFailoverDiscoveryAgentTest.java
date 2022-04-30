package org.apache.activemq.network;

public class StaticFailoverDiscoveryAgentTest extends NetworkConnectionsTest {
    @Override
    protected NetworkConnector addNetworkConnector() throws Exception {
        return localBroker.addNetworkConnector("staticfailover:(" + REMOTE_BROKER_TRANSPORT_URI + "," + REMOTE_BROKER_TRANSPORT_URI + ")");
    }
}