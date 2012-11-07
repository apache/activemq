package org.apache.activemq.leveldb;

import junit.framework.Test;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.XARecoveryBrokerTest;

import java.io.File;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LevelDBXARecoveryBrokerTest extends XARecoveryBrokerTest {

    public static Test suite() {
        return suite(LevelDBXARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        LevelDBStore store = new LevelDBStore();
        store.setDirectory(new File("target/activemq-data/xahaleveldb"));
        broker.setPersistenceAdapter(store);
    }

}
