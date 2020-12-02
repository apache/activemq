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
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Ignore;
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
    ArrayList<ElectingLevelDBStore> stores = new ArrayList<ElectingLevelDBStore>();
    ElectingLevelDBStore master = null;

    @Ignore("https://issues.apache.org/jira/browse/AMQ-5512")
    @Test(timeout = 1000*60*10)
    public void testElection() throws Exception {
        deleteDirectory("leveldb-node1");
        deleteDirectory("leveldb-node2");
        deleteDirectory("leveldb-node3");


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
        ArrayList<String> messagesInStore = getMessages(ms);
        int index=0;
        for (String id: expected_list) {
            if (!id.equals(messagesInStore.get(index))) {
                LOG.info("Mismatch for expected:" + id + ", got:" + messagesInStore.get(index));
                break;
            }
            index++;
        }
        assertEquals(expected_list, messagesInStore);
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
    }

    /*
     * testAMQ5082 tests the behavior of an ElectingLevelDBStore
     * pool when ZooKeeper I/O timeouts occur. See issue AMQ-5082.
     */
    @Test(timeout = 1000 * 60 * 5)
    public void testAMQ5082() throws Throwable {
        final ArrayList<ElectingLevelDBStore> stores = new ArrayList<ElectingLevelDBStore>();

        LOG.info("Launching 3 stores");
        for (String dir : new String[]{"leveldb-node1", "leveldb-node2", "leveldb-node3"}) {
            ElectingLevelDBStore store = createStoreNode();
            store.setDirectory(new File(data_dir(), dir));
            stores.add(store);
            asyncStart(store);
        }

        LOG.info("Waiting 30s for stores to start");
        Thread.sleep(30 * 1000);

        LOG.info("Checking for a single master");
        ElectingLevelDBStore master = null;
        for (ElectingLevelDBStore store: stores) {
            if (store.isMaster()) {
                assertNull(master);
                master = store;
            }
        }
        assertNotNull(master);

        LOG.info("Imposing 1s I/O wait on Zookeeper connections, waiting 30s to confirm that quorum is not lost");
        this.connector.testHandle.setIOWaitMillis(1 * 1000, 30 * 1000);

        LOG.info("Confirming that the quorum has not been lost");
        for (ElectingLevelDBStore store: stores) {
            if (store.isMaster()) {
                assertTrue(master == store);
            }
        }

        LOG.info("Imposing 11s I/O wait on Zookeeper connections, waiting 30s for quorum to be lost");
        this.connector.testHandle.setIOWaitMillis(11 * 1000, 60 * 1000);

        LOG.info("Confirming that the quorum has been lost");
        for (ElectingLevelDBStore store: stores) {
            assertFalse(store.isMaster());
        }
        master = null;

        LOG.info("Lifting I/O wait on Zookeeper connections, waiting 30s for quorum to be re-established");
        this.connector.testHandle.setIOWaitMillis(0, 30 * 1000);

        LOG.info("Checking for a single master");
        for (ElectingLevelDBStore store: stores) {
            if (store.isMaster()) {
                assertNull(master);
                master = store;
            }
        }
        assertNotNull(master);
    }

    @After
    public void stop() throws Exception {
        if (master != null) {
            master.stop();
            FileUtils.deleteDirectory(master.directory());
        }
        for(ElectingLevelDBStore store: stores) {
            store.stop();
            FileUtils.deleteDirectory(store.directory());
        }
        stores.clear();
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
        store.setSync("quorum_disk");
        store.setZkSessionTimeout("15s");
        store.setZkAddress("localhost:" + connector.getLocalPort());
        store.setZkPath("/broker-stores");
        store.setBrokerName("foo");
        store.setHostname("localhost");
        store.setBind("tcp://0.0.0.0:0");
        return store;
    }

}
