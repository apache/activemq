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
package org.apache.activemq.kaha.impl.index.hash;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.TestCase;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.util.IOHelper;

/**
 * Test a HashIndex
 */
public class HashTest extends TestCase {

    private static final int COUNT = 10000;

    private HashIndex hashIndex;

    private File directory;

    private IndexManager indexManager;

    /**
     * @throws java.lang.Exception
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        directory = new File(IOHelper.getDefaultDataDirectory());
        directory.mkdirs();
        IOHelper.deleteChildren(directory);
        indexManager = new IndexManager(directory, "im-hash-test", "rw", null,
                new AtomicLong());
        this.hashIndex = new HashIndex(directory, "testHash", indexManager);
        this.hashIndex.setNumberOfBins(12);
        this.hashIndex.setPageSize(32 * 1024);
        this.hashIndex.setKeyMarshaller(Store.STRING_MARSHALLER);
    }

    public void testHashIndex() throws Exception {
        doTest(300);
        hashIndex.clear();
        hashIndex.unload();
        doTest(600);
        hashIndex.clear();
        hashIndex.unload();
        doTest(128);
    }

    public void doTest(int pageSize) throws Exception {
        String keyRoot = "key:";
        hashIndex.setPageSize(pageSize);
        this.hashIndex.load();
        doInsert(keyRoot);
        this.hashIndex.unload();
        this.hashIndex.load();
        checkRetrieve(keyRoot);
        doRemove(keyRoot);
        this.hashIndex.unload();
        this.hashIndex.load();
        doInsert(keyRoot);
        doRemoveHalf(keyRoot);
        doInsertHalf(keyRoot);
        this.hashIndex.unload();
        this.hashIndex.load();
        checkRetrieve(keyRoot);
        this.hashIndex.unload();
    }

    void doInsert(String keyRoot) throws Exception {
        for (int i = 0; i < COUNT; i++) {
            IndexItem value = indexManager.createNewIndex();
            indexManager.storeIndex(value);
            hashIndex.store(keyRoot + i, value);
        }
    }

    void checkRetrieve(String keyRoot) throws IOException {
        for (int i = 0; i < COUNT; i++) {
            IndexItem item = (IndexItem) hashIndex.get(keyRoot + i);
            assertNotNull(item);
        }
    }

    void doRemoveHalf(String keyRoot) throws Exception {
        for (int i = 0; i < COUNT; i++) {
            if (i % 2 == 0) {
                hashIndex.remove(keyRoot + i);
            }

        }
    }

    void doInsertHalf(String keyRoot) throws Exception {
        for (int i = 0; i < COUNT; i++) {
            if (i % 2 == 0) {
                IndexItem value = indexManager.createNewIndex();
                indexManager.storeIndex(value);
                hashIndex.store(keyRoot + i, value);
            }
        }
    }

    void doRemove(String keyRoot) throws Exception {
        for (int i = 0; i < COUNT; i++) {
            hashIndex.remove(keyRoot + i);
        }
        for (int i = 0; i < COUNT; i++) {
            IndexItem item = (IndexItem) hashIndex.get(keyRoot + i);
            assertNull(item);
        }
    }

    void doRemoveBackwards(String keyRoot) throws Exception {
        for (int i = COUNT - 1; i >= 0; i--) {
            hashIndex.remove(keyRoot + i);
        }
        for (int i = 0; i < COUNT; i++) {
            IndexItem item = (IndexItem) hashIndex.get(keyRoot + i);
            assertNull(item);
        }
    }

    /**
     * @throws java.lang.Exception
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        File[] files = directory.listFiles();
        for (File file : files) {
            file.delete();
        }
    }
}
