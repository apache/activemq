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
import org.junit.Test;

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

    private void executeTest(String preallocationStrategy)throws Exception {
        Random rand = new Random();
        int randInt = rand.nextInt(100);
        File dataDirectory = new File("./target/activemq-data/kahadb" + randInt);

        KahaDBStore store = new KahaDBStore();
        store.deleteAllMessages();
        store.setDirectory(dataDirectory);
        store.setPreallocationStrategy(preallocationStrategy);
        store.start();

        // time for files to get there.. i know this is a brittle test! need to find
        // a better way (callbacks?) to notify when the journal is completely up
        TimeUnit.MILLISECONDS.sleep(500);
        File journalLog = new File(dataDirectory, "db-1.log");
        assertTrue(journalLog.exists());


        FileInputStream is = new FileInputStream(journalLog);
        FileChannel channel = is.getChannel();
        assertEquals(Journal.DEFAULT_MAX_FILE_LENGTH, channel.size());

        channel.position(1 * 1024 * 1024 + 1);
        ByteBuffer buff = ByteBuffer.allocate(1);
        channel.read(buff);
        buff.flip();
        buff.position(0);
        assertEquals(0x00, buff.get());

        System.out.println("File size: " + channel.size());


        store.stop();
    }


}
