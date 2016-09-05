/*
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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the single threaded DataFileAppender class.
 */
public class TargetedDataFileAppenderTest {

    private Journal dataManager;
    private TargetedDataFileAppender appender;
    private DataFile dataFile;
    private File dir;

    @Before
    public void setUp() throws Exception {
        dir = new File("target/tests/TargetedDataFileAppenderTest");
        dir.mkdirs();
        dataManager = new Journal();
        dataManager.setDirectory(dir);
        dataManager.start();

        dataFile = dataManager.reserveDataFile();
        appender = new TargetedDataFileAppender(dataManager, dataFile);
    }

    @After
    public void tearDown() throws Exception {
        dataManager.close();
        IOHelper.delete(dir);
    }

    @Test
    public void testWritesAreBatched() throws Exception {
        final int iterations = 10;
        ByteSequence data = new ByteSequence("DATA".getBytes());
        for (int i = 0; i < iterations; i++) {
            appender.storeItem(data, Journal.USER_RECORD_TYPE, false);
        }

        assertTrue("Data file should not be empty", dataFile.getLength() > 0);
        assertTrue("Data file should be empty", dataFile.getFile().length() == 0);

        appender.close();

        // at this point most probably dataManager.getInflightWrites().size() >= 0
        // as the Thread created in DataFileAppender.enqueue() may not have caught up.
        assertTrue("Data file should not be empty", dataFile.getLength() > 0);
        assertTrue("Data file should not be empty", dataFile.getFile().length() > 0);
    }

    @Test
    public void testBatchWritesCompleteAfterClose() throws Exception {
        final int iterations = 10;
        ByteSequence data = new ByteSequence("DATA".getBytes());
        for (int i = 0; i < iterations; i++) {
            appender.storeItem(data, Journal.USER_RECORD_TYPE, false);
        }

        appender.close();

        // at this point most probably dataManager.getInflightWrites().size() >= 0
        // as the Thread created in DataFileAppender.enqueue() may not have caught up.
        assertTrue("Data file should not be empty", dataFile.getLength() > 0);
        assertTrue("Data file should not be empty", dataFile.getFile().length() > 0);
    }

    @Test
    public void testBatchWriteCallbackCompleteAfterClose() throws Exception {
        final int iterations = 10;
        final CountDownLatch latch = new CountDownLatch(iterations);
        ByteSequence data = new ByteSequence("DATA".getBytes());
        for (int i = 0; i < iterations; i++) {
            appender.storeItem(data, Journal.USER_RECORD_TYPE, new Runnable() {
                @Override
                public void run() {
                    latch.countDown();
                }
            });
        }

        appender.close();

        // at this point most probably dataManager.getInflightWrites().size() >= 0
        // as the Thread created in DataFileAppender.enqueue() may not have caught up.
        assertTrue("queued data is written", latch.await(5, TimeUnit.SECONDS));
        assertTrue("Data file should not be empty", dataFile.getLength() > 0);
        assertTrue("Data file should not be empty", dataFile.getFile().length() > 0);
    }
}
