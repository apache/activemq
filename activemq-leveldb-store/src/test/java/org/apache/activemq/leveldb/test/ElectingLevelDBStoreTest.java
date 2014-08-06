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
package org.apache.activemq.leveldb.test;

import org.apache.activemq.Service;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.leveldb.CountDownFuture;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;
import org.apache.activemq.store.MessageStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import static org.apache.activemq.leveldb.test.ReplicationTestSupport.*;
import static org.junit.Assert.*;

/**
 */
public class ElectingLevelDBStoreTest extends ZooKeeperTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(ElectingLevelDBStoreTest.class);

    @Test(timeout = 1000*60*10)
    public void testElection() throws Exception {

        ArrayList<ElectingLevelDBStore> stores = new ArrayList<ElectingLevelDBStore>();
        ArrayList<CountDownFuture> pending_starts = new ArrayList<CountDownFuture>();

        for(String dir: new String[]{"leveldb-node1", "leveldb-node2", "leveldb-node3"}) {
            ElectingLevelDBStore store = createStoreNode();
            store.setDirectory(new File(data_dir(), dir));
            stores.add(store);
            pending_starts.add(asyncStart(store));
        }

        // At least one of the stores should have started.
        CountDownFuture f = waitFor(30 * 1000, pending_starts.toArray(new CountDownFuture[pending_starts.size()]));
        assertTrue(f!=null);
        pending_starts.remove(f);

        // The other stores should not start..
        LOG.info("Making sure the other stores don't start");
        Thread.sleep(5000);
        for(CountDownFuture start: pending_starts) {
            assertFalse(start.completed());
        }

        // Make sure only of the stores is reporting to be the master.
        ElectingLevelDBStore master = null;
        for(ElectingLevelDBStore store: stores) {
            if( store.isMaster() ) {
                assertNull(master);
                master = store;
            }
        }
        assertNotNull(master);

        // We can work out who the slaves are...
        HashSet<ElectingLevelDBStore> slaves = new HashSet<ElectingLevelDBStore>(stores);
        slaves.remove(master);

        // Start sending messages to the master.
        ArrayList<String> expected_list = new ArrayList<String>();
        MessageStore ms = master.createQueueMessageStore(new ActiveMQQueue("TEST"));
        final int TOTAL = 500;
        for (int i = 0; i < TOTAL; i++) {
            if (i % ((int) (TOTAL * 0.10)) == 0) {
                LOG.info("" + (100 * i / TOTAL) + "% done");
            }

            if( i == 250 ) {

                LOG.info("Checking master state");
                assertEquals(expected_list, getMessages(ms));

                // mid way, lets kill the master..
                LOG.info("Killing Master.");
                master.stop();

                // At least one of the remaining stores should complete starting.
                LOG.info("Waiting for slave takeover...");
                f = waitFor(60 * 1000, pending_starts.toArray(new CountDownFuture[pending_starts.size()]));
                assertTrue(f!=null);
                pending_starts.remove(f);

                // Make sure one and only one of the slaves becomes the master..
                master = null;
                for(ElectingLevelDBStore store: slaves) {
                    if( store.isMaster() ) {
                        assertNull(master);
                        master = store;
                    }
                }

                assertNotNull(master);
                slaves.remove(master);

                ms = master.createQueueMessageStore(new ActiveMQQueue("TEST"));
            }

            String msgid = "m:" + i;
            addMessage(ms, msgid);
            expected_list.add(msgid);
        }

        LOG.info("Checking master state");
        assertEquals(expected_list, getMessages(ms));

        master.stop();
        for(ElectingLevelDBStore store: stores) {
            store.stop();
        }
    }

    @Test(timeout = 1000 * 60 * 10)
    public void testZooKeeperServerFailure() throws Exception {

        final ArrayList<ElectingLevelDBStore> stores = new ArrayList<ElectingLevelDBStore>();
        ArrayList<CountDownFuture> pending_starts = new ArrayList<CountDownFuture>();

        for (String dir : new String[]{"leveldb-node1", "leveldb-node2", "leveldb-node3"}) {
            ElectingLevelDBStore store = createStoreNode();
            store.setDirectory(new File(data_dir(), dir));
            stores.add(store);
            pending_starts.add(asyncStart(store));
        }

        // At least one of the stores should have started.
        CountDownFuture f = waitFor(30 * 1000, pending_starts.toArray(new CountDownFuture[pending_starts.size()]));
        assertTrue(f != null);
        pending_starts.remove(f);

        // The other stores should not start..
        LOG.info("Making sure the other stores don't start");
        Thread.sleep(5000);
        for (CountDownFuture start : pending_starts) {
            assertFalse(start.completed());
        }

        // Stop ZooKeeper..
        LOG.info("SHUTTING DOWN ZooKeeper!");
        connector.shutdown();

        // None of the store should be slaves...
        within( 30, TimeUnit.SECONDS, new Task(){
            public void run() throws Exception {
                for (ElectingLevelDBStore store : stores) {
                    assertFalse(store.isMaster());
                }
            }
        });

        for (ElectingLevelDBStore store : stores) {
            store.stop();
        }
    }

    private CountDownFuture asyncStart(final Service service) {
        final CountDownFuture<Throwable> f = new CountDownFuture<Throwable>();
        LevelDBStore.BLOCKING_EXECUTOR().execute(new Runnable() {
            public void run() {
                try {
                    service.start();
                    f.set(null);
                } catch (Throwable e) {
                    e.printStackTrace();
                    f.set(e);
                }
            }
        });
        return f;
    }

    private CountDownFuture asyncStop(final Service service) {
        final CountDownFuture<Throwable> f = new CountDownFuture<Throwable>();
        LevelDBStore.BLOCKING_EXECUTOR().execute(new Runnable() {
            public void run() {
                try {
                    service.stop();
                    f.set(null);
                } catch (Throwable e) {
                    e.printStackTrace();
                    f.set(e);
                }
            }
        });
        return f;
    }

    private ElectingLevelDBStore createStoreNode() {
        ElectingLevelDBStore store = new ElectingLevelDBStore();
        store.setSecurityToken("foo");
        store.setLogSize(1024 * 200);
        store.setReplicas(2);
        store.setZkAddress("localhost:" + connector.getLocalPort());
        store.setZkPath("/broker-stores");
        store.setBrokerName("foo");
        store.setHostname("localhost");
        store.setBind("tcp://0.0.0.0:0");
        return store;
    }

}
