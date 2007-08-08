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
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.activeio.journal.InvalidRecordLocationException;
import org.apache.activeio.journal.RecordLocation;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activemq.kaha.impl.async.JournalFacade.RecordLocationFacade;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Tests the AsyncDataManager based Journal
 * 
 * @version $Revision: 1.1 $
 */
public class JournalImplTest extends TestCase {

    Log log = LogFactory.getLog(JournalImplTest.class);

    int size = 1024 * 10;
    int logFileCount = 2;
    File logDirectory = new File("target/dm-data2");
    private JournalFacade journal;

    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        if (logDirectory.exists()) {
            deleteDir(logDirectory);
        }
        assertTrue("Could not delete directory: " + logDirectory.getCanonicalPath(), !logDirectory.exists());
        AsyncDataManager dm = new AsyncDataManager();
        dm.setDirectory(logDirectory);
        dm.setMaxFileLength(1024 * 64);
        dm.start();
        journal = new JournalFacade(dm);
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
        journal.close();
        if (logDirectory.exists())
            deleteDir(logDirectory);
        // assertTrue( !logDirectory.exists() );
    }

    public void testLogFileCreation() throws IOException {
        RecordLocation mark = journal.getMark();
        assertNull(mark);
    }

    @SuppressWarnings("unchecked")
    public void testAppendAndRead() throws InvalidRecordLocationException, InterruptedException, IOException {

        Packet data1 = createPacket("Hello World 1");
        RecordLocation location1 = journal.write(data1, false);
        Packet data2 = createPacket("Hello World 2");
        RecordLocation location2 = journal.write(data2, false);
        Packet data3 = createPacket("Hello World 3");
        RecordLocation location3 = journal.write(data3, false);

        // Thread.sleep(1000);

        // Now see if we can read that data.
        Packet data;
        data = journal.read(location2);
        assertEquals(data2, data);
        data = journal.read(location1);
        assertEquals(data1, data);
        data = journal.read(location3);
        assertEquals(data3, data);

        // Can we cursor the data?
        RecordLocation l = journal.getNextRecordLocation(null);
        int t = l.compareTo(location1);
        assertEquals(0, t);
        data = journal.read(l);
        assertEquals(data1, data);

        l = journal.getNextRecordLocation(l);
        assertEquals(0, l.compareTo(location2));
        data = journal.read(l);
        assertEquals(data2, data);

        l = journal.getNextRecordLocation(l);
        assertEquals(0, l.compareTo(location3));
        data = journal.read(l);
        assertEquals(data3, data);

        l = journal.getNextRecordLocation(l);
        assertNull(l);

        log.info(journal);
    }

    public void testCanReadFromArchivedLogFile() throws InvalidRecordLocationException, InterruptedException, IOException {

        Packet data1 = createPacket("Hello World 1");
        RecordLocationFacade location1 = (RecordLocationFacade)journal.write(data1, false);

        RecordLocationFacade pos;
        int counter = 0;
        do {

            Packet p = createPacket("<<<data>>>");
            pos = (RecordLocationFacade)journal.write(p, false);
            if (counter++ % 1000 == 0) {
                journal.setMark(pos, false);
            }

        } while (pos.getLocation().getDataFileId() < 5);

        // Now see if we can read that first packet.
        Packet data;
        data = journal.read(location1);
        assertEquals(data1, data);

    }

    /**
     * @param string
     * @return
     */
    private Packet createPacket(String string) {
        return new ByteArrayPacket(string.getBytes());
    }

    public static void assertEquals(Packet arg0, Packet arg1) {
        assertEquals(arg0.sliceAsBytes(), arg1.sliceAsBytes());
    }

    public static void assertEquals(byte[] arg0, byte[] arg1) {

        // System.out.println("Comparing: "+new String(arg0)+" and "+new
        // String(arg1));
        if (arg0 == null ^ arg1 == null)
            fail("Not equal: " + arg0 + " != " + arg1);
        if (arg0 == null)
            return;
        if (arg0.length != arg1.length)
            fail("Array lenght not equal: " + arg0.length + " != " + arg1.length);
        for (int i = 0; i < arg0.length; i++) {
            if (arg0[i] != arg1[i]) {
                fail("Array item not equal at index " + i + ": " + arg0[i] + " != " + arg1[i]);
            }
        }
    }
}
