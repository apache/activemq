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
package org.apache.activemq.kaha.impl.async;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.activemq.util.ByteSequence;

public class DataFileAppenderTest extends TestCase {
    AsyncDataManager dataManager;
    File dir;
    
    @Override
    public void setUp() throws Exception {
        dir = new File("target/tests/DataFileAppenderTest");
        dir.mkdirs();
        dataManager = new AsyncDataManager();
        dataManager.setDirectory(dir);
        configure(dataManager);
        dataManager.start();
    }
    
    protected void configure(AsyncDataManager dataManager) {
        dataManager.setUseNio(false);
    }

    @Override
    public void tearDown() throws Exception {
        dataManager.close();
        deleteFilesInDirectory(dir);
        dir.delete();
    }

    private void deleteFilesInDirectory(File directory) {
        File[] files = directory.listFiles();
        for (int i=0; i<files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                deleteFilesInDirectory(f);
            }   
            f.delete();
        }  
    }
    
    public void testBatchWriteCompleteAfterTimeout() throws Exception {
        ByteSequence data = new ByteSequence("DATA".getBytes());
        final int iterations = 10;
        for (int i=0; i<iterations; i++) {
            dataManager.write(data, false);
        }
        assertTrue("writes are queued up", dataManager.getInflightWrites().size() >= iterations);
        Thread.sleep(1000);
        assertTrue("queued data is written", dataManager.getInflightWrites().isEmpty());
    }


    public void testBatchWriteCallbackCompleteAfterTimeout() throws Exception {
        final int iterations = 10;
        final CountDownLatch latch = new CountDownLatch(iterations);
        ByteSequence data = new ByteSequence("DATA".getBytes());
        for (int i=0; i<iterations; i++) {
            dataManager.write(data, new Runnable() {
                public void run() {
                    latch.countDown();                 
                }
            });
        }
        assertTrue("writes are queued up", dataManager.getInflightWrites().size() >= iterations);
        assertEquals("none written", iterations, latch.getCount());
        Thread.sleep(1000);
        assertTrue("queued data is written", dataManager.getInflightWrites().isEmpty());
        assertEquals("none written", 0, latch.getCount());
    }

    public void testBatchWriteCallbackCompleteAfterClose() throws Exception {
        final int iterations = 10;
        final CountDownLatch latch = new CountDownLatch(iterations);
        ByteSequence data = new ByteSequence("DATA".getBytes());
        for (int i=0; i<iterations; i++) {
            dataManager.write(data, new Runnable() {
                public void run() {
                    latch.countDown();                 
                }
            });
        }
        assertTrue("writes are queued up", dataManager.getInflightWrites().size() >= iterations);
        assertEquals("none written", iterations, latch.getCount());
        dataManager.close();
        assertTrue("queued data is written", dataManager.getInflightWrites().isEmpty());
        assertEquals("none written", 0, latch.getCount());
    }
    
    public void testBatchWriteCompleteAfterClose() throws Exception {
        ByteSequence data = new ByteSequence("DATA".getBytes());
        final int iterations = 10;
        for (int i=0; i<iterations; i++) {
            dataManager.write(data, false);
        }
        dataManager.close();
        assertTrue("queued data is written:" + dataManager.getInflightWrites().size(), dataManager.getInflightWrites().isEmpty());
    }
    
    public void testBatchWriteToMaxMessageSize() throws Exception {
        final int iterations = 4;
        final CountDownLatch latch = new CountDownLatch(iterations);
        Runnable done = new Runnable() {
            public void run() {
                latch.countDown();                 
            }
        };
        int messageSize = DataFileAppender.DEFAULT_MAX_BATCH_SIZE / iterations;
        byte[] message = new byte[messageSize];
        ByteSequence data = new ByteSequence(message);
        
        for (int i=0; i< iterations - 1; i++) {
            dataManager.write(data, done);
        }
        assertEquals("all writes are queued", iterations, latch.getCount());
        dataManager.write(data, done);
        latch.await(10, TimeUnit.SECONDS); // write may take some time
        assertEquals("all callbacks complete", 0, latch.getCount());
    }
    
    public void testNoBatchWriteWithSync() throws Exception {
        ByteSequence data = new ByteSequence("DATA".getBytes());
        final int iterations = 10;
        for (int i=0; i<iterations; i++) {
            dataManager.write(data, true);
            assertTrue("queued data is written", dataManager.getInflightWrites().isEmpty());
        }
    }
}
