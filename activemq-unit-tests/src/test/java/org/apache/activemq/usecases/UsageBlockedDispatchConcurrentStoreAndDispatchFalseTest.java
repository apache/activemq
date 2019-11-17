package org.apache.activemq.usecases;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import java.io.IOException;

public class UsageBlockedDispatchConcurrentStoreAndDispatchFalseTest  extends UsageBlockedDispatchTest {
    @Override
    protected BrokerService createBroker() throws IOException {
        BrokerService broker = new BrokerService();
        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setConcurrentStoreAndDispatchQueues(false);
        broker.setPersistenceAdapter(kahadb);
        return broker;
    }
}
