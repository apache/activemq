package org.apache.activemq.network;

import java.net.URI;

/**
 * Abstraction that allows you to control which brokers a NetworkConnector connects bridges to.
 * 
 * @version $Revision$
 */
public interface ConnectionFilter {
    /**
     * @param location
     * @return true if the network connector should establish a connection to the specified location.
     */
    boolean connectTo(URI location);
}
