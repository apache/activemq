package org.apache.activemq.transport.tcp;

/**
 * Thrown to indicate that the {@link TcpTransportServer#maximumConnections} 
 * property has been exceeded. 
 * 
 * @see {@link TcpTransportServer#maximumConnections}
 * @author bsnyder
 *
 */
public class ExceededMaximumConnectionsException extends Exception {

    /**
     * Default serial version id for serialization
     */
    private static final long serialVersionUID = -1166885550766355524L;

    public ExceededMaximumConnectionsException(String message) {
        super(message);
    }

}
