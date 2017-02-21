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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataFileAppenderNoSpaceNoBatchTest {
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
            }
        }

        assertEquals("got 2 seeks: " + seekPositions, 2, seekPositions.size());
        assertEquals("offset is reused", seekPositions.get(0), seekPositions.get(1));

    }
}
