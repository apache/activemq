package org.apache.activemq.xbean;

import java.net.URI;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class XBeanStartFalseTest extends TestCase {
    
    public void testStartFalse() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/xbean/activemq2.xml"));
        assertFalse("Broker is started", broker.isStarted());
    }

}
