package org.activemq.broker;

import java.io.IOException;
import java.net.URISyntaxException;

import org.activemq.store.PersistenceAdapter;

public class BrokerRestartTestSupport extends BrokerTestSupport {

    private PersistenceAdapter persistenceAdapter;

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        persistenceAdapter = broker.getPersistenceAdapter();
        return broker;
    }

    /**
     * @return 
     * @throws Exception
     */
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistenceAdapter(persistenceAdapter);
        return broker;
    }

    /**
     * Simulates a broker restart.  The memory based persistence adapter is
     * reused so that it does not "loose" it's "persistent" messages.
     * 
     * @throws IOException
     * @throws URISyntaxException 
     */
    protected void restartBroker() throws Exception {
        broker.stop();
        broker = createRestartedBroker();
        broker.start();
    }


}
