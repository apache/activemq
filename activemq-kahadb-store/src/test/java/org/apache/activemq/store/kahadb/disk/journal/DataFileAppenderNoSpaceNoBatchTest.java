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

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataFileAppenderNoSpaceNoBatchTest {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileAppenderNoSpaceNoBatchTest.class);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private DataFileAppender underTest;

    @Test
    public void testNoSpaceNextWriteSameBatch() throws Exception {
        final List<Long> seekPositions = Collections.synchronizedList(new ArrayList<Long>());

        final DataFile currentDataFile = new DataFile(dataFileDir.newFile(), 0) {
            public RecoverableRandomAccessFile appendRandomAccessFile() throws IOException {

                return new RecoverableRandomAccessFile(dataFileDir.newFile(), "rw") {

                    public void seek(long pos) throws IOException {
                        seekPositions.add(pos);
                    }

                    public void write(byte[] bytes, int offset, int len) throws IOException {
                        throw new IOException("No space on device");
                    }
                };
            };
        };

        underTest = new DataFileAppender(new Journal() {
            @Override
            public DataFile getCurrentDataFile(int capacity) throws IOException {
                return currentDataFile;
            };
        });

        final ByteSequence byteSequence = new ByteSequence(new byte[4*1024]);
        for (int i=0; i<2; i++) {
            try {
                underTest.storeItem(byteSequence, (byte) 1, true);
                fail("expect no space");
            } catch (IOException expected) {
                underTest.shutdown = false;
            }
        }

        assertEquals("got 2 seeks: " + seekPositions, 2, seekPositions.size());
        assertEquals("offset is reused", seekPositions.get(0), seekPositions.get(1));

    }

    @Test
    public void testSingleNoSpaceNextWriteSameBatch() throws Exception {
        final List<Long> seekPositions = Collections.synchronizedList(new ArrayList<Long>());

        final DataFile currentDataFile = new DataFile(dataFileDir.newFile(), 0) {
            public RecoverableRandomAccessFile appendRandomAccessFile() throws IOException {

                return new RecoverableRandomAccessFile(dataFileDir.newFile(), "rw") {

                    public void seek(long pos) throws IOException {
                        seekPositions.add(pos);
                    }

                    public void write(byte[] bytes, int offset, int len) throws IOException {
                        throw new IOException("No space on device");
                    }
                };
            };
        };

        underTest = new DataFileAppender(new Journal() {
            @Override
            public DataFile getCurrentDataFile(int capacity) throws IOException {
                return currentDataFile;
            };
        });

        final ByteSequence byteSequence = new ByteSequence(new byte[4*1024]);
        for (int i=0; i<2; i++) {
            try {
                underTest.storeItem(byteSequence, (byte) 1, true);
                fail("expect no space");
            } catch (IOException expected) {
            }
        }

        assertEquals("got 1 seeks: " + seekPositions, 1, seekPositions.size());
    }

    @Test(timeout = 10000)
    public void testNoSpaceNextWriteSameBatchAsync() throws Exception {
        final List<Long> seekPositions = Collections.synchronizedList(new ArrayList<Long>());

        final DataFile currentDataFile = new DataFile(dataFileDir.newFile(), 0) {
            public RecoverableRandomAccessFile appendRandomAccessFile() throws IOException {

                return new RecoverableRandomAccessFile(dataFileDir.newFile(), "rw") {

                    public void seek(long pos) throws IOException {
                        seekPositions.add(pos);
                    }

                    public void write(byte[] bytes, int offset, int len) throws IOException {
                        if (seekPositions.size() == 2) {
                            throw new IOException("No space on device: " + seekPositions.size());
                        }
                    }
                };
            };
        };

        underTest = new DataFileAppender(new Journal() {
            @Override
            public DataFile getCurrentDataFile(int capacity) throws IOException {
                return currentDataFile;
            };

            @Override
            public int getWriteBatchSize() {
                // force multiple async batches
                return 4*1024;
            }
        });

        final ByteSequence byteSequence = new ByteSequence(new byte[1024]);

        ConcurrentLinkedQueue<Location> locations = new ConcurrentLinkedQueue<Location>();
        HashSet<CountDownLatch> latches = new HashSet<CountDownLatch>();
        for (int i = 0; i <= 20; i++) {
            try {
                Location location = underTest.storeItem(byteSequence, (byte) 1, false);
                locations.add(location);
                latches.add(location.getLatch());
            } catch (IOException expected) {
                underTest.shutdown = false;
            }
        }

        for (CountDownLatch latch: latches) {
            assertTrue("write complete", latch.await(5, TimeUnit.SECONDS));
        }

        boolean someExceptions = false;
        for (Location location: locations) {
            someExceptions |= (location.getException().get() != null);
        }
        assertTrue(someExceptions);

        LOG.info("Latches count: " + latches.size());
        LOG.info("Seeks: " + seekPositions);

        assertTrue("got more than on latch: " + latches.size(), latches.size() > 1);
        assertTrue("got seeks: " + seekPositions, seekPositions.size() > 2);
        assertEquals("no duplicates: " + seekPositions, seekPositions.size(), new HashSet<Long>(seekPositions).size());
    }
}
