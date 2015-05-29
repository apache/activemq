package org.apache.activemq.xbean;

import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.junit.Assert;
import org.junit.Test;

public class DestinationBridgeAccessorsTest extends Assert {

    @Test
    public void testCreateBroker() throws Exception {
        XBeanBrokerFactory xBeanBrokerFactory = new XBeanBrokerFactory();

        BrokerService broker = xBeanBrokerFactory.createBroker(new URI("handleReplyToActivemq.xml"));

        assertNotNull(broker);
    }

}