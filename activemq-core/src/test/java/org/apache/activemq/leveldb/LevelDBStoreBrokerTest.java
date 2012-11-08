package org.apache.activemq.leveldb;

import junit.framework.Test;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.activemq.store.PersistenceAdapter;

import java.io.File;
import java.io.IOException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LevelDBStoreBrokerTest extends BrokerTest {

    public static Test suite() {
        return suite(LevelDBStoreBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistenceAdapter(createPersistenceAdapter(true));
        return broker;
    }

    protected PersistenceAdapter createPersistenceAdapter(boolean delete) {
        LevelDBStore store  = new LevelDBStore();
        store.setDirectory(new File("target/activemq-data/leveldb"));
        if (delete) {
          store.deleteAllMessages();
        }
        return store;
      }

      protected BrokerService createRestartedBroker() throws IOException {
        BrokerService broker = new BrokerService();
        broker.setPersistenceAdapter(createPersistenceAdapter(false));
        return broker;
      }

}
