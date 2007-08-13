package org.apache.activemq.transport;

/**
 * MBean to manage a single Transport Logger.
 * It can inform if the logger is currently writing to a log file or not,
 * by setting the logging property or by using the operations
 * enableLogging() and disableLogging()
 */
public interface TransportLoggerViewMBean {

    /**
     * Returns if the managed TransportLogger is currently active
     * (writing to a log) or not.
     * @return if the managed TransportLogger is currently active
     * (writing to a log) or not.
     */
    public boolean isLogging();
    
    /**
     * Enables or disables logging for the managed TransportLogger.
     * @param logging Boolean value to enable or disable logging for
     * the managed TransportLogger.
     * true to enable logging, false to disable logging.
     */
    public void setLogging(boolean logging);
    
    /**
     * Enables logging for the managed TransportLogger.
     */
    public void enableLogging();
    
    /**
     * Disables logging for the managed TransportLogger.
     */
    public void disableLogging();
    
}
