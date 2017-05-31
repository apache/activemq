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
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.activemq.store.AbstractMessageStoreSizeTest;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.util.LocationMarshaller;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.Test;

/**
 * This test is for AMQ-5748 to verify that {@link MessageStore} implements correctly
 * compute the size of the messages in the store.
 *
 * For KahaDB specifically, the size was not being stored in in the index ({@link LocationMarshaller}).  LocationMarshaller
 * has been updated to include an option to include the size in the serialized value.  This way the message
 * size will be persisted in the index and be available between broker restarts without needing to rebuild the index.
 * Note that the KahaDB version has been incremented from 5 to 6 because the index will need to be rebuild when a version
 * 5 index is detected since it will be detected as corrupt.
 *
 */
public abstract class AbstractKahaDBMessageStoreSizeTest extends AbstractMessageStoreSizeTest {

    MessageStore messageStore;
    PersistenceAdapter store;

    @Override
    public void initStore() throws Exception {
        createStore(true, dataDirectory);
    }

    abstract protected void createStore(boolean deleteAllMessages, String directory) throws Exception;

    abstract protected String getVersion5Dir();

    @Override
    public void destroyStore() throws Exception {
        if (store != null) {
            store.stop();
        }
    }


    /**
     * This method tests that the message sizes exist for all messages that exist after messages are recovered
     * off of disk.
     *
     * @throws Exception
     */
    @Test
    public void testMessageSizeStoreRecovery() throws Exception {
        writeMessages();
        store.stop();

        createStore(false, dataDirectory);
        writeMessages();
        long messageSize = messageStore.getMessageSize();
        assertEquals(40, messageStore.getMessageCount());
        assertTrue(messageSize > 40 * testMessageSize);
    }

    /**
     * This method tests that a version 5 store with an old index still works but returns 0 for messgage sizes.
     *
     * @throws Exception
     */
    @Test
    public void testMessageSizeStoreRecoveryVersion5() throws Exception {
        store.stop();

        //Copy over an existing version 5 store with messages
        File dataDir = new File(dataDirectory);
        if (dataDir.exists())
            FileUtils.deleteDirectory(new File(dataDirectory));
        FileUtils.copyDirectory(new File(getVersion5Dir()),
                dataDir);

        //reload store
        createStore(false, dataDirectory);

        //make sure size is 0
        long messageSize = messageStore.getMessageSize();
        assertTrue(messageStore.getMessageCount() == 20);
        assertTrue(messageSize == 0);


    }

    /**
     * This method tests that a version 5 store with existing messages will correctly be recovered and converted
     * to version 6.  After index deletion, the index will be rebuilt and will include message sizes.
     *
     * @throws Exception
     */
    @Test
    public void testMessageSizeStoreRecoveryVersion5RebuildIndex() throws Exception {
        store.stop();

        //Copy over an existing version 5 store with messages
        File dataDir = new File(dataDirectory);
        if (dataDir.exists())
            FileUtils.deleteDirectory(new File(dataDirectory));
        FileUtils.copyDirectory(new File(getVersion5Dir()),
                dataDir);
        for (File index : FileUtils.listFiles(new File(dataDirectory), new WildcardFileFilter("*.data"), TrueFileFilter.INSTANCE)) {
            FileUtils.deleteQuietly(index);
        }

        //append more messages...at this point the index should be rebuilt
        createStore(false, dataDirectory);
        writeMessages();

        //after writing new messages to the existing store, make sure the index is rebuilt and size is correct
        long messageSize = messageStore.getMessageSize();
        assertTrue(messageStore.getMessageCount() == 40);
        assertTrue(messageSize > 40 * testMessageSize);

    }

    @Override
    protected MessageStore getMessageStore() {
        return messageStore;
    }

}
