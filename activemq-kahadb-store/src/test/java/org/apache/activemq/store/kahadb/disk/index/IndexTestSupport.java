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
package org.apache.activemq.store.kahadb.disk.index;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Test;

/**
 * Test a HashIndex
 */
public abstract class IndexTestSupport extends TestCase {

    private static final int COUNT = 10000;

    protected Index<String,Long> index;
    protected File directory;
    protected PageFile pf;
    protected Transaction tx;

    @Override
	@After
    public void tearDown() throws Exception {
        if( pf!=null ) {
            pf.unload();
            pf.delete();
        }
    }

    public File getDirectory() {
        if (directory != null) {
            IOHelper.delete(directory);
        }
        directory = new File(IOHelper.getDefaultDataDirectory() + System.currentTimeMillis());
        IOHelper.delete(directory);
        return directory;
    }

    protected void createPageFileAndIndex(int pageSize) throws Exception {
        pf = new PageFile(getDirectory(), getClass().getName());
        pf.setPageSize(pageSize);
        pf.load();
        tx = pf.tx();
        this.index = createIndex();
    }

    abstract protected Index<String, Long> createIndex() throws Exception;

    @Test(timeout=60000)
    public void testIndex() throws Exception {
        createPageFileAndIndex(500);
        this.index.load(tx);
        tx.commit();
        doInsert(COUNT);
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();
        checkRetrieve(COUNT);
        doRemove(COUNT);
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();
        doInsert(COUNT);
        doRemoveHalf(COUNT);
        doInsertHalf(COUNT);
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();
        checkRetrieve(COUNT);
        this.index.unload(tx);
        tx.commit();
    }

    void doInsert(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            index.put(tx, key(i), (long)i);
            tx.commit();
        }
    }

    protected String key(int i) {
        return "key:"+i;
    }

    void checkRetrieve(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            Long item = index.get(tx, key(i));
            assertNotNull("Key missing: "+key(i), item);
        }
    }

    void doRemoveHalf(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                assertNotNull("Expected remove to return value for index "+i, index.remove(tx, key(i)));
                tx.commit();
            }

        }
    }

    void doInsertHalf(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                index.put(tx, key(i), (long)i);
                tx.commit();
            }
        }
    }

    void doRemove(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            assertNotNull("Expected remove to return value for index "+i, index.remove(tx, key(i)));
            tx.commit();
        }
        for (int i = 0; i < count; i++) {
            Long item = index.get(tx, key(i));
            assertNull(item);
        }
    }

    void doRemoveBackwards(int count) throws Exception {
        for (int i = count - 1; i >= 0; i--) {
            index.remove(tx, key(i));
            tx.commit();
        }
        for (int i = 0; i < count; i++) {
            Long item = index.get(tx, key(i));
            assertNull(item);
        }
    }
}
