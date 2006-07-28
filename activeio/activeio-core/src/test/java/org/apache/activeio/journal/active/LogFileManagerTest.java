/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.journal.active;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.activeio.journal.InvalidRecordLocationException;
import org.apache.activeio.journal.active.BatchedWrite;
import org.apache.activeio.journal.active.Location;
import org.apache.activeio.journal.active.LogFileManager;
import org.apache.activeio.journal.active.Record;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.ByteBufferPacket;

/**
 * Tests the LogFile used by JournalImpl
 * 
 * @version $Revision: 1.1 $
 */
public class LogFileManagerTest extends TestCase {

    int size = 1024 * 512;

    int logFileCount = 4;

    File logDirectory = new File("test-logfile");

    private LogFileManager logFile;

    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        if (logDirectory.exists()) {
            deleteDir(logDirectory);
        }
        assertTrue(!logDirectory.exists());
        logFile = new LogFileManager(logDirectory, logFileCount, size, null);
    }

    /**
     */
    private void deleteDir(File f) {
        File[] files = f.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            file.delete();
        }
        f.delete();
    }

    protected void tearDown() throws Exception {
        logFile.dispose();
        if (logDirectory.exists())
            deleteDir(logDirectory);
        assertTrue(!logDirectory.exists());
    }

    public void testLogFileCreation() throws IOException {
        assertTrue(logFile.canActivateNextLogFile());
        assertEquals(null,logFile.getFirstActiveLogLocation());
        assertNull(logFile.getLastMarkedRecordLocation());
        assertEquals(new Location(0, 0),logFile.getNextAppendLocation());
    }

    public void testAppendAndRead() throws IOException, InvalidRecordLocationException, InterruptedException {

        System.out.println("Initial:"+logFile.getNextAppendLocation());
        appendHelloRecord(1001);
        Location loc2 = logFile.getNextAppendLocation();
        appendHelloRecord(2002);
        appendHelloRecord(3003);
        appendHelloRecord(3004);

        Location loc3 = logFile.getNextDataRecordLocation(loc2);
        assertTrue(loc3.getLogFileOffset() > loc2.getLogFileOffset());
        Location loc4 = logFile.getNextDataRecordLocation(loc3);
        assertTrue(loc4.getLogFileOffset() > loc3.getLogFileOffset());

    }

    public void testRollOver() throws IOException, InvalidRecordLocationException, InterruptedException {

        int lastId = logFile.getNextAppendLocation().getLogFileId();
        int counter = 0;
        for (int i = 0; i < logFileCount; i++) {
            counter += 500;
            appendHelloRecord(counter);
            if (i + 1 == logFileCount) {
                assertFalse(logFile.canActivateNextLogFile());
            } else {
                assertTrue(logFile.canActivateNextLogFile());
                logFile.activateNextLogFile();
                assertEquals(lastId + 1, logFile.getNextAppendLocation().getLogFileId());
                lastId = logFile.getNextAppendLocation().getLogFileId();
            }
        }

    }

    /**
     * @param i
     * @throws IOException
     * @throws InterruptedException
     */
    private void appendHelloRecord(int i) throws IOException, InterruptedException {
        byte data[] = ("Hello World: " + i).getBytes();
        Record batchedRecord = new Record(LogFileManager.DATA_RECORD_TYPE, new ByteArrayPacket(data), null);
        batchedRecord.setLocation(logFile.getNextAppendLocation());
        
        BatchedWrite write = new BatchedWrite(new ByteBufferPacket(ByteBuffer.allocate(1024)));
        write.append(batchedRecord,null, true);
        write.flip();
        logFile.append(write);
    }
}
