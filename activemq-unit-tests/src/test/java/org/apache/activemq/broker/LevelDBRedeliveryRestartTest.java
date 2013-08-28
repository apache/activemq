package org.apache.activemq.broker;

import junit.framework.Test;
import org.apache.activemq.leveldb.LevelDBStore;

import java.io.IOException;

/**
 */
public class LevelDBRedeliveryRestartTest extends RedeliveryRestartTest {
    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        broker.setDestinationPolicy(policyMap);
        LevelDBStore store = new LevelDBStore();
        broker.setPersistenceAdapter(store);
        broker.addConnector("tcp://0.0.0.0:0");
    }

    @Override
    protected void stopBrokerWithStoreFailure() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    public static Test suite() {
        return suite(LevelDBRedeliveryRestartTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
