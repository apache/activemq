package org.apache.activemq.network;

import org.apache.activemq.transport.Transport;

/**
 * Encapsulation of bridge creation logic.
 *
 * This SPI interface is intended to customize or decorate existing bridge implementations.
 */
public interface BridgeFactory {

    /**
     * Create a network bridge between two specified transports.
     *
     * @param configuration Bridge configuration.
     * @param localTransport Local side of bridge.
     * @param remoteTransport Remote side of bridge.
     * @param listener Bridge listener.
     * @return the NetworkBridge
     */
    DemandForwardingBridge createNetworkBridge(NetworkBridgeConfiguration configuration, Transport localTransport, Transport remoteTransport, final NetworkBridgeListener listener);

}
