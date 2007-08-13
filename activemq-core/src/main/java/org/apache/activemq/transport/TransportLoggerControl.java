package org.apache.activemq.transport;

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ManagementContext;

/**
 * Implementation of the TransportLoggerControlMBean interface,
 * which is an MBean used to control all TransportLoggers at once.
 */
public class TransportLoggerControl implements TransportLoggerControlMBean {

    /**
     * Constructor
     */
    public TransportLoggerControl(ManagementContext managementContext) {
    }

    // doc comment inherited from TransportLoggerControlMBean
    public void disableAllTransportLoggers() {
        TransportLoggerView.disableAllTransportLoggers();
    }

    // doc comment inherited from TransportLoggerControlMBean
    public void enableAllTransportLoggers() {
        TransportLoggerView.enableAllTransportLoggers();
    }

    //  doc comment inherited from TransportLoggerControlMBean
    public void reloadLog4jProperties() throws Exception {
        new BrokerView(null, null).reloadLog4jProperties();
    }

}
