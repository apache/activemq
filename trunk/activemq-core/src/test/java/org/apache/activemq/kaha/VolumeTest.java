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
import java.util.Iterator;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolumeTest extends TestCase {

    protected static final int NUMBER = 1;
    private static final transient Logger LOG = LoggerFactory.getLogger(VolumeTest.class);

    protected Store store;
    protected String name;

    /*
     * dump a large number of messages into a list - then retreive them
     */
    public void testListVolume() throws Exception {
        ListContainer container = store.getListContainer("volume");
        container.setMarshaller(Store.BYTES_MARSHALLER);
        byte[] data = new byte[10];
        for (int i = 0; i < NUMBER; i++) {
            container.add(data);
            if (i % 100000 == 0) {
                LOG.error("persisted " + i);
            }

        }
        int count = 0;

        for (Iterator i = container.iterator(); i.hasNext();) {
            assertNotNull(i.next());
            count++;
            if (count % 100000 == 0) {
                LOG.error("retrived  " + count);
            }
        }
        assertEquals("Different retrieved to stored", NUMBER, count);
    }

    protected Store getStore() throws IOException {
        return StoreFactory.open(name, "rw");
    }

    protected void setUp() throws Exception {
        super.setUp();
        name = System.getProperty("basedir", ".") + "/target/activemq-data/volume-container.db";
        StoreFactory.delete(name);
        store = StoreFactory.open(name, "rw");

    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (store != null) {
            store.close();
        }
        assertTrue(StoreFactory.delete(name));
    }
}
