/*
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

package org.apache.activemq.broker.region.cursors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.BiConsumer;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.usage.SystemUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class StoreCursorRemoveFromCacheTest {

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder();

    private final ActiveMQQueue destination = new ActiveMQQueue("queue");
    private BrokerService broker;
    private SystemUsage systemUsage;
    private KahaDBStore store;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(true);
        KahaDBStore store = new KahaDBStore();
        store.setDirectory(dataFileDir.getRoot());
        broker.setPersistenceAdapter(store);
        broker.start();
        systemUsage = broker.getSystemUsage();
        this.store = (KahaDBStore) broker.getPersistenceAdapter();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test(timeout = 10000)
    public void testRemoveFromCacheIterator() throws Exception {
        testRemoveFromCache((cursor, ref) -> {
            // test removing using the iterator
            cursor.remove();
        });
    }

    @Test(timeout = 10000)
    public void testRemoveFromCacheRemoveMethod() throws Exception {
        testRemoveFromCache((cursor, ref) -> {
            // test using the remove method directly
            // remove should also decrement after AMQ-9698, previously it did not
            cursor.remove(ref);
            assertEquals(0, ref.getReferenceCount());

            // Call a second time to make sure we don't go negative
            // and it will skip
            cursor.remove(ref);
            assertEquals(0, ref.getReferenceCount());
        });
    }

    private void testRemoveFromCache(BiConsumer<QueueStorePrefetch, MessageReference> remove) throws Exception {
        var systemUsage = broker.getSystemUsage();
        final KahaDBStore store = (KahaDBStore) broker.getPersistenceAdapter();
        final MessageStore messageStore = store.createQueueMessageStore(destination);
        final Queue queue = new Queue(broker, destination, messageStore, new DestinationStatistics(), null);
        var memoryUsage = queue.getMemoryUsage();

        // create cursor and make sure cache is enabled
        QueueStorePrefetch cursor = new QueueStorePrefetch(queue, broker.getBroker());
        cursor.setSystemUsage(systemUsage);
        cursor.start();
        assertTrue("cache enabled", cursor.isUseCache() && cursor.isCacheEnabled());

        for (int i = 0; i < 10; i++) {
            ActiveMQTextMessage msg = getMessage(i);
            msg.setMemoryUsage(memoryUsage);
            cursor.addMessageLast(msg);
            // reference count of 1 for the cache
            assertEquals(1, msg.getReferenceCount());
        }

        assertTrue(memoryUsage.getUsage() > 0);

        cursor.reset();
        while (cursor.hasNext()) {
            // next will increment again so need to decrement the reference
            // next is required to be called for remove() to work
            var ref = cursor.next();
            assertEquals(2, ref.getReferenceCount());
            ref.decrementReferenceCount();
            remove.accept(cursor, ref);
            assertEquals(0, ref.getReferenceCount());
        }

        assertEquals(0, memoryUsage.getUsage());
        assertEquals(0, cursor.size());
    }

    private ActiveMQTextMessage getMessage(int i) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        MessageId id = new MessageId("11111:22222:" + i);
        id.setBrokerSequenceId(i);
        id.setProducerSequenceId(i);
        message.setMessageId(id);
        message.setDestination(destination);
        message.setPersistent(true);
        message.setResponseRequired(true);
        message.setText("Msg:" + i + " " + "test");
        assertEquals(message.getMessageId().getProducerSequenceId(), i);
        return message;
    }

}
