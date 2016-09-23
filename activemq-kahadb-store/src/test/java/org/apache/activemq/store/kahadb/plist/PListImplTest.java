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
package org.apache.activemq.store.kahadb.plist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.PListTestSupport;
import org.apache.activemq.util.IOHelper;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PListImplTest extends PListTestSupport {


    @Override
    protected PListStoreImpl createPListStore() {
        return new PListStoreImpl();
    }

    @Override
    protected PListStore createConcurrentAddIteratePListStore() {
        PListStoreImpl store = createPListStore();
        store.setIndexPageSize(2 * 1024);
        store.setJournalMaxFileLength(1024 * 1024);
        store.setCleanupInterval(-1);
        store.setIndexEnablePageCaching(false);
        store.setIndexWriteBatchSize(100);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddRemovePListStore() {
        PListStoreImpl store = createPListStore();
        store.setCleanupInterval(400);
        store.setJournalMaxFileLength(1024*5);
        store.setLazyInit(false);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddRemoveWithPreloadPListStore() {
        PListStoreImpl store = createPListStore();
        store.setJournalMaxFileLength(1024*5);
        store.setCleanupInterval(5000);
        store.setIndexWriteBatchSize(500);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddIterateRemovePListStore(boolean enablePageCache) {
        PListStoreImpl store = createPListStore();
        store.setIndexEnablePageCaching(enablePageCache);
        store.setIndexPageSize(2*1024);
        return store;
    }

    @Test
    public void testIndexDir() throws Exception {
        PListStoreImpl pListStore = (PListStoreImpl)store;
        assertEquals(pListStore.getDirectory(), pListStore.getIndexDirectory());
    }

    @Test
    public void testSetIndexDir() throws Exception {
        PListStoreImpl pListStore = (PListStoreImpl)store;
        final File directory = pListStore.getDirectory();
        pListStore.stop();
        pListStore = createPListStore();
        pListStore.setDirectory(directory);
        pListStore.setLazyInit(false);
        pListStore.setIndexDirectory(new File(directory, "indexDir"));
        pListStore.start();
        assertNotEquals(pListStore.getDirectory(), pListStore.getIndexDirectory());
        pListStore.stop();
    }

    //Test that when lazy init is true that the directory gets cleaned up on start up
    @Test
    public void testLazyInitCleanup() throws Exception {
        PListStoreImpl pListStore = (PListStoreImpl)store;
        File directory = pListStore.getDirectory();
        File indexDir = tempFolder.newFolder();
        pListStore.stop();

        //Restart one time with index directory so everything gets created
        pListStore = createPListStore();
        pListStore.setLazyInit(false);
        pListStore.setDirectory(directory);
        pListStore.setIndexDirectory(indexDir);
        pListStore.start();
        pListStore.stop();

        assertTrue(directory.exists());
        assertTrue(indexDir.exists());

        //restart again with lazy init true and make sure that the directories are cleared
        pListStore = createPListStore();
        pListStore.setLazyInit(true);
        pListStore.setDirectory(directory);
        pListStore.setIndexDirectory(indexDir);

        //assert that start cleaned up old data
        pListStore.start();
        assertFalse(directory.exists());
        assertFalse(indexDir.exists());

        //assert that initialize re-created the data dirs
        pListStore.intialize();
        assertTrue(directory.exists());
        assertTrue(indexDir.exists());
        pListStore.stop();

    }
}
