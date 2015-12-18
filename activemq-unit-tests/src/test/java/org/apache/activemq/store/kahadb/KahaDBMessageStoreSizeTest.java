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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.MessageDatabase.MessageKeys;
import org.apache.activemq.store.kahadb.MessageDatabase.StoredDestination;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.junit.Test;

/**
 * This test is for AMQ-5748 to verify that {@link MessageStore} implements correctly
 * compute the size of the messages in the KahaDB Store.
 *
 */
public class KahaDBMessageStoreSizeTest extends AbstractKahaDBMessageStoreSizeTest {

    @Override
    protected void createStore(boolean deleteAllMessages, String directory) throws Exception {
        KahaDBStore kahaDBStore = new KahaDBStore();
        store = kahaDBStore;
        kahaDBStore.setJournalMaxFileLength(1024 * 512);
        kahaDBStore.setDeleteAllMessages(deleteAllMessages);
        kahaDBStore.setDirectory(new File(directory));
        kahaDBStore.start();
        messageStore = store.createQueueMessageStore(destination);
        messageStore.start();
    }

    /**
     * Make sure that the sizes stored in the KahaDB location index are the same as in
     * the message order index.
     *
     * @throws Exception
     */
    @Test
    public void testLocationIndexMatchesOrderIndex() throws Exception {
        final KahaDBStore kahaDbStore = (KahaDBStore) store;
        writeMessages();

        //Iterate over the order index and add up the size of the messages to compare
        //to the location index
        kahaDbStore.indexLock.readLock().lock();
        try {
            long size = kahaDbStore.pageFile.tx().execute(new Transaction.CallableClosure<Long, IOException>() {
                @Override
                public Long execute(Transaction tx) throws IOException {
                    long size = 0;

                    // Iterate through all index entries to get the size of each message
                    StoredDestination sd = kahaDbStore.getStoredDestination(kahaDbStore.convert(destination), tx);
                    for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                        size += iterator.next().getValue().location.getSize();
                    }
                   return size;
                }
            });
            assertEquals("Order index size values don't match message size",
                    size, messageStore.getMessageSize());
        } finally {
            kahaDbStore.indexLock.readLock().unlock();
        }
    }

    @Override
    protected String getVersion5Dir() {
        return "src/test/resources/org/apache/activemq/store/kahadb/MessageStoreTest/version5";
    }
}
