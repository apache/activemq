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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.leveldb.LevelDBStoreView;
import org.apache.activemq.leveldb.util.FileSupport;
import org.apache.activemq.store.MessageStore;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.activemq.leveldb.test.ReplicationTestSupport.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexRebuildTest {
    protected static final Logger LOG = LoggerFactory.getLogger(IndexRebuildTest.class);
    final int max = 30;
    final int toLeave = 5;
    ArrayList<LevelDBStore> stores = new ArrayList<LevelDBStore>();

    @Test(timeout = 1000 * 60 * 10)
    public void testRebuildIndex() throws Exception {

        File masterDir = new File("target/activemq-data/leveldb-rebuild");
        FileSupport.toRichFile(masterDir).recursiveDelete();

        final LevelDBStore store = new LevelDBStore();
        store.setDirectory(masterDir);
        store.setLogDirectory(masterDir);

        store.setLogSize(1024 * 10);
        store.start();
        stores.add(store);

        ArrayList<MessageId> inserts = new ArrayList<MessageId>();
        MessageStore ms = store.createQueueMessageStore(new ActiveMQQueue("TEST"));
        for (int i = 0; i < max; i++) {
            inserts.add(addMessage(ms, "m" + i).getMessageId());
        }
        int logFileCount = countLogFiles(store);
        assertTrue("more than one journal file", logFileCount > 1);

        for (MessageId id : inserts.subList(0, inserts.size() - toLeave)) {
            removeMessage(ms, id);
        }

        LevelDBStoreView view = new LevelDBStoreView(store);
        view.compact();

        int reducedLogFileCount = countLogFiles(store);
        assertTrue("log files deleted", logFileCount > reducedLogFileCount);

        store.stop();

        deleteTheIndex(store);

        assertEquals("log files remain", reducedLogFileCount, countLogFiles(store));

        // restart, recover and verify message read
        store.start();
        ms = store.createQueueMessageStore(new ActiveMQQueue("TEST"));

        assertEquals(toLeave + " messages remain", toLeave, getMessages(ms).size());
    }

    private void deleteTheIndex(LevelDBStore store) throws IOException {
        for (String index : store.getLogDirectory().list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                LOG.info("dir:" + dir + ", name: " + name);
                return (name != null && name.endsWith(".index"));
            }
        })) {

            File file = new File(store.getLogDirectory().getAbsoluteFile(), index);
            LOG.info("Deleting index directory:" + file);
            FileUtils.deleteDirectory(file);
        }

    }

    private int countLogFiles(LevelDBStore store) {
        return store.getLogDirectory().list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                LOG.info("dir:" + dir + ", name: " + name);
                return (name != null && name.endsWith(".log"));
            }
        }).length;
    }

    @After
    public void stop() throws Exception {
        for (LevelDBStore store : stores) {
            if (store.isStarted()) {
                store.stop();
            }
            FileUtils.deleteDirectory(store.directory());
        }
        stores.clear();
    }

}
