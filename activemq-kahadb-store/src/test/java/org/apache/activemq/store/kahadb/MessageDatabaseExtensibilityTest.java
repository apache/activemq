/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the KahaDB extension points added for external subclasses
 * (GH-2140): {@link KahaDBPersistenceAdapter#createStore()} lets an
 * adapter supply a {@link KahaDBStore} subclass, and the protected
 * {@code updateIndex} methods let that
 * subclass see index updates without copying store code. The
 * override below only counts invocations and delegates; the broker
 * must behave identically to the stock store.
 */
public class MessageDatabaseExtensibilityTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    static class CountingStore extends KahaDBStore {
        final AtomicInteger indexedAdds = new AtomicInteger();

        @Override
        protected long updateIndex(Transaction tx, KahaAddMessageCommand command,
                Location location) throws IOException {
            indexedAdds.incrementAndGet();
            return super.updateIndex(tx, command, location);
        }
    }

    static class CountingAdapter extends KahaDBPersistenceAdapter {
        // createStore() is invoked from the superclass field initializer,
        // before this subclass's own field initializers run, so the store
        // must be constructed here, not in a field initializer.
        CountingStore store;

        @Override
        protected KahaDBStore createStore() {
            store = new CountingStore();
            return store;
        }
    }

    @Test
    public void adapterSuppliedStoreSubclassObservesIndexUpdates() throws Exception {
        var kahadbDir = temporaryFolder.newFolder("kahadb");
        var broker = new BrokerService();
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setDataDirectoryFile(kahadbDir.getParentFile());

        var adapter = new CountingAdapter();
        adapter.setDirectory(kahadbDir);
        adapter.setCheckpointInterval(0);
        adapter.setCleanupInterval(0);
        // Deterministic counting: with concurrent store-and-dispatch the
        // index update is asynchronous relative to send().
        adapter.setConcurrentStoreAndDispatchQueues(false);
        broker.setPersistenceAdapter(adapter);
        broker.start();
        broker.waitUntilStarted();

        try {
            var cf = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
            try (var connection = cf.createConnection()) {
                connection.start();
                var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                var queue = session.createQueue("EXTENSIBILITY.TEST");
                var producer = session.createProducer(queue);
                for (var i = 0; i < 25; i++) {
                    producer.send(session.createTextMessage("msg-" + i));
                }

                // The subclass saw every persistent add.
                assertEquals(25, adapter.store.indexedAdds.get());

                // The Metadata accessors (added for cross-package
                // subclasses, which cannot reach the nested class's fields)
                // return the updated location.
                assertNotNull("metadata lastUpdate should be set after messages are stored",
                        adapter.store.metadata.getLastUpdate());

                // Behavior is unchanged: all messages are consumable.
                try (var consumer = session.createConsumer(queue)) {
                    for (var i = 0; i < 25; i++) {
                        var received = consumer.receive(10_000);
                        assertNotNull("message " + i + " must be delivered", received);
                    }
                }
            }
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }
}
