package org.apache.activemq.network;

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.command.Message;

public class CompositeNetworkBridgeListener implements NetworkBridgeListener {
    private final List<NetworkBridgeListener> listeners;

    public CompositeNetworkBridgeListener(NetworkBridgeListener... wrapped) {
        this.listeners = Arrays.asList(wrapped);
    }

    @Override
    public void bridgeFailed() {
        for (NetworkBridgeListener listener : listeners) {
            listener.bridgeFailed();
        }
    }

    @Override
    public void onStart(NetworkBridge bridge) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onStart(bridge);
        }
    }

    @Override
    public void onStop(NetworkBridge bridge) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onStop(bridge);
        }
    }

    @Override
    public void onOutboundMessage(NetworkBridge bridge, Message message) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onOutboundMessage(bridge, message);
        }
    }

    @Override
    public void onInboundMessage(NetworkBridge bridge, Message message) {
        for (NetworkBridgeListener listener : listeners) {
            listener.onInboundMessage(bridge, message);
        }
    }
}
