package org.activemq.openwire;

import junit.framework.TestCase;

public class ItStillMarshallsTheSameTest extends TestCase {

    public void testAll() throws Exception {
        BrokerInfoData.assertAllControlFileAreEqual();
    }
    
}
