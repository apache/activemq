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
package org.apache.activemq.kaha.impl.index.tree;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;

/**
 * Test a TreeIndex
 */
public class TreeTest extends TestCase {

    private static final int COUNT = 55;
    private TreeIndex tree;
    private File directory;
    private IndexManager indexManager;
    private boolean dumpTree;

    /**
     * @throws java.lang.Exception
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        directory = new File("activemq-data");
        directory.mkdirs();
        indexManager = new IndexManager(directory, "im-test", "rw", null,new AtomicLong());
        this.tree = new TreeIndex(directory, "testTree", indexManager);
        this.tree.setKeyMarshaller(Store.STRING_MARSHALLER);
    }

    public void testTreeWithCaching() throws Exception {
        this.tree.setEnablePageCaching(true);
        // doTest();
    }

    public void testTreeWithoutCaching() throws Exception {
        this.tree.setEnablePageCaching(false);
        // doTest();
    }

    public void doTest() throws Exception {
        // doTest(300);
        // tree.clear();
        // tree.unload();
        // count = 55 - this fails
        doTest(600);
        // tree.clear();
        // tree.unload();
        // doTest(1024*16);
    }

    public void doTest(int pageSize) throws Exception {
        String keyRoot = "key:";
        tree.setPageSize(pageSize);
        this.tree.load();
        // doInsert(keyRoot);
        // checkRetrieve(keyRoot);
        // doRemove(keyRoot);
        doInsert(keyRoot);
        doRemoveBackwards(keyRoot);
    }

    void doInsert(String keyRoot) throws Exception {
        for (int i = 0; i < COUNT; i++) {
            IndexItem value = indexManager.createNewIndex();
            indexManager.storeIndex(value);
            tree.store(keyRoot + i, value);
        }
    }

    void checkRetrieve(String keyRoot) throws IOException {
        for (int i = 0; i < COUNT; i++) {
            IndexItem item = (IndexItem)tree.get(keyRoot + i);
            assertNotNull(item);
        }
    }

    void doRemove(String keyRoot) throws Exception {
        for (int i = 0; i < COUNT; i++) {
            tree.remove(keyRoot + i);
            // System.out.println("Removed " + keyRoot+i);
            // tree.getRoot().dump();
            // System.out.println("");
        }
        for (int i = 0; i < COUNT; i++) {
            IndexItem item = (IndexItem)tree.get(keyRoot + i);
            assertNull(item);
        }
    }

    void doRemoveBackwards(String keyRoot) throws Exception {
        for (int i = COUNT - 1; i >= 0; i--) {
            tree.remove(keyRoot + i);
            System.out.println("BACK Removed " + keyRoot + i);
            tree.getRoot().dump();
            System.out.println("");
        }
        for (int i = 0; i < COUNT; i++) {
            IndexItem item = (IndexItem)tree.get(keyRoot + i);
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
