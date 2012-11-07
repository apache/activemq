package org.apache.activemq.transport;

import org.apache.activemq.TransportLoggerSupport;

import java.io.IOException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportLoggerFactorySPI implements TransportLoggerSupport.SPI {
    @Override
    public Transport createTransportLogger(Transport transport) throws IOException {
        return TransportLoggerFactory.getInstance().createTransportLogger(transport);
    }

    @Override
    public Transport createTransportLogger(Transport transport, String logWriterName, boolean dynamicManagement, boolean startLogging, int jmxPort) throws IOException {
        return TransportLoggerFactory.getInstance().createTransportLogger(transport, logWriterName, dynamicManagement, startLogging, jmxPort);
    }
}
