package org.apache.activemq.network;

import org.apache.activemq.transport.Transport;
import org.mockito.Mockito;

public class CustomNetworkBridgeFactory implements BridgeFactory { 

    private final NetworkBridgeListener listener;

    public CustomNetworkBridgeFactory() {
        this(Mockito.mock(NetworkBridgeListener.class));
    }

    public CustomNetworkBridgeFactory(NetworkBridgeListener listener) {
        this.listener = listener;
    }

    public NetworkBridgeListener getListener() {
        return listener;
    }

    @Override
    public DemandForwardingBridge createNetworkBridge(NetworkBridgeConfiguration configuration, Transport localTransport, Transport remoteTransport, NetworkBridgeListener listener) {
        DemandForwardingBridge bridge = new DemandForwardingBridge(configuration, localTransport, remoteTransport);
        bridge.setNetworkBridgeListener(new CompositeNetworkBridgeListener(this.listener, listener));
        return bridge;
    }

 }
