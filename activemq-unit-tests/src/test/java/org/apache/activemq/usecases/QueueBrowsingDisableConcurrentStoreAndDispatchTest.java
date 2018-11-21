package org.apache.activemq.usecases;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import java.io.File;
import java.io.IOException;

public class QueueBrowsingDisableConcurrentStoreAndDispatchTest extends QueueBrowsingTest {
    @Override
    public BrokerService createBroker() throws IOException {
        BrokerService broker = super.createBroker();
        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setConcurrentStoreAndDispatchQueues(false);
        broker.setPersistenceAdapter(kahadb);
        return broker;
    }
}
