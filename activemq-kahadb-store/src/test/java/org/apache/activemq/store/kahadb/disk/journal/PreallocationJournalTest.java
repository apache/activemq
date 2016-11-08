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
package org.apache.activemq.store.kahadb.disk.journal;

import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.store.kahadb.data.KahaTraceCommand;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public class PreallocationJournalTest  {

    private static final Logger LOG = LoggerFactory.getLogger(PreallocationJournalTest.class);

    @Test
    public void testSparseFilePreallocation() throws Exception {
        executeTest("sparse_file");
    }

    @Test
    public void testOSCopyPreallocation() throws Exception {
        executeTest("os_kernel_copy");
    }

    @Test
    public void testZerosPreallocation() throws Exception {
        executeTest("zeros");
    }

    @Test
    public void testZerosLoop() throws Exception {
        Random rand = new Random();
        int randInt = rand.nextInt(100);
        File dataDirectory = new File("./target/activemq-data/kahadb" + randInt);

        KahaDBStore store = new KahaDBStore();
        store.setJournalMaxFileLength(5*1024*1024);
        store.deleteAllMessages();
        store.setDirectory(dataDirectory);
        store.setPreallocationStrategy("zeros");
        store.start();

        final File journalLog = new File(dataDirectory, "db-1.log");
        assertTrue("file exists", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return journalLog.exists();
            }
        }));


        KahaTraceCommand traceCommand = new KahaTraceCommand();
        traceCommand.setMessage(new String(new byte[2*1024*1024]));
        Location location = null;
        for (int i=0; i<20; i++) {
            location = store.store(traceCommand);
        }
        LOG.info("Last location:" + location);

        LOG.info("Store journal files:" + store.getJournal().getFiles().size());

    }

    private void executeTest(String preallocationStrategy)throws Exception {
        Random rand = new Random();
        int randInt = rand.nextInt(100);
        File dataDirectory = new File("./target/activemq-data/kahadb" + randInt);

        KahaDBStore store = new KahaDBStore();
        store.deleteAllMessages();
        store.setDirectory(dataDirectory);
        store.setPreallocationStrategy(preallocationStrategy);
        store.start();

        final File journalLog = new File(dataDirectory, "db-1.log");
        assertTrue("file exists", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return journalLog.exists();
            }
        }));


        FileInputStream is = new FileInputStream(journalLog);
        final FileChannel channel = is.getChannel();
        assertTrue("file size as expected", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info ("file size:" + journalLog + ", chan.size " + channel.size() + ", jfileSize.length: " + journalLog.length());
                return Journal.DEFAULT_MAX_FILE_LENGTH == channel.size();
            }
        }));

        channel.position(1 * 1024 * 1024 + 1);
        ByteBuffer buff = ByteBuffer.allocate(1);
        channel.read(buff);
        buff.flip();
        buff.position(0);
        assertEquals(0x00, buff.get());

        LOG.info("File size: " + channel.size());

        store.stop();
    }


}
