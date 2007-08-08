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
package org.apache.activemq.kaha;

import java.io.IOException;
import java.io.PrintWriter;
import junit.framework.TestCase;
import org.apache.activemq.kaha.impl.KahaStore;
import java.util.concurrent.CountDownLatch;

/**
 * Store test
 * 
 * @version $Revision: 1.2 $
 */
public class LoadTest extends TestCase {
    static final int COUNT = 10000;
    static final int NUM_LOADERS = 5;
    protected String name = "load.db";
    protected KahaStore store;

    /*
     * Test method for 'org.apache.activemq.kaha.Store.close()'
     */
    public void testLoad() throws Exception {
        CountDownLatch start = new CountDownLatch(NUM_LOADERS);
        CountDownLatch stop = new CountDownLatch(NUM_LOADERS);
        for (int i = 0; i < NUM_LOADERS; i++) {
            Loader loader = new Loader("loader:" + i, store, COUNT, start, stop);
            loader.start();
        }
        stop.await();
    }

    protected KahaStore getStore() throws IOException {
        return (KahaStore)StoreFactory.open(name, "rw");
    }

    protected void setUp() throws Exception {
        super.setUp();
        name = System.getProperty("basedir", ".") + "/target/activemq-data/load.db";
        StoreFactory.delete(name);
        store = getStore();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        store.clear();
        store.close();
        assertTrue(StoreFactory.delete(name));
    }
}
