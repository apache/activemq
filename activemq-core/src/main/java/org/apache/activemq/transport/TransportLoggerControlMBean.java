package org.apache.activemq.transport;

/**
 * MBean used to manage all of the TransportLoggers at once.
 * Avalaible operations:
 *  -Enable logging for all TransportLoggers at once.
 *  -Disable logging for all TransportLoggers at once.
 */
public interface TransportLoggerControlMBean {

    /**
     * Enable logging for all Transport Loggers at once.
     */
    public void enableAllTransportLoggers();

    /**
     * Disable logging for all Transport Loggers at once.
     */
    public void disableAllTransportLoggers();
    
    /**
     * Reloads log4j.properties from the classpath
     * @throws Exception
     */
    public void reloadLog4jProperties() throws Exception;
    
}
