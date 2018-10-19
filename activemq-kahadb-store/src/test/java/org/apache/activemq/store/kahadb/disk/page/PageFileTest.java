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
package org.apache.activemq.store.kahadb.disk.page;

import junit.framework.TestCase;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("rawtypes")
public class PageFileTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(PageFileTest.class);

    public void testCRUD() throws IOException {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        HashSet<String> expected = new HashSet<String>();

        // Insert some data into the page file.
        Transaction tx = pf.tx();
        for (int i = 0; i < 100; i++) {
            Page<String> page = tx.allocate();
            assertEquals(Page.PAGE_FREE_TYPE, page.getType());

            String t = "page:" + i;
            expected.add(t);
            page.set(t);
            tx.store(page, StringMarshaller.INSTANCE, false);
            tx.commit();
        }

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure they are still there..
        HashSet<String> actual = new HashSet<String>();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add(page.get());
        }
        assertEquals(expected, actual);

        // Remove the odd records..
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                break;
            }
            String t = "page:" + i;
            expected.remove(t);
        }
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            if (!expected.contains(page.get())) {
                tx.free(page);
            }
        }
        tx.commit();

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure the even records are still there..
        actual.clear();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add((String)page.get());
        }
        assertEquals(expected, actual);

        // Update the records...
        HashSet<String> t = expected;
        expected = new HashSet<String>();
        for (String s : t) {
            expected.add(s + ":updated");
        }
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            page.set(page.get() + ":updated");
            tx.store(page, StringMarshaller.INSTANCE, false);
        }
        tx.commit();

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure the updated records are still there..
        actual.clear();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add(page.get());
        }
        assertEquals(expected, actual);

        pf.unload();
    }

    public void testStreams() throws IOException {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        Transaction tx = pf.tx();
        Page page = tx.allocate();

        OutputStream pos = tx.openOutputStream(page, true);
        DataOutputStream os = new DataOutputStream(pos);
        for (int i = 0; i < 10000; i++) {
            os.writeUTF("Test string:" + i);
        }

        os.close();
        tx.commit();

        // Reload the page file.
        pf.unload();
        pf.load();
        tx = pf.tx();

        InputStream pis = tx.openInputStream(page);
        DataInputStream is = new DataInputStream(pis);
        for (int i = 0; i < 10000; i++) {
            assertEquals("Test string:" + i, is.readUTF());
        }
        assertEquals(-1, is.read());
        is.close();

        pf.unload();
    }

    public void testAddRollback() throws IOException {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        HashSet<String> expected = new HashSet<String>();

        // Insert some data into the page file.
        Transaction tx = pf.tx();
        for (int i = 0; i < 100; i++) {
            Page<String> page = tx.allocate();
            assertEquals(Page.PAGE_FREE_TYPE, page.getType());

            String t = "page:" + i;
            page.set(t);
            tx.store(page, StringMarshaller.INSTANCE, false);

            // Rollback every other insert.
            if (i % 2 == 0) {
                expected.add(t);
                tx.commit();
            } else {
                tx.rollback();
            }

        }

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure they are still there..
        HashSet<String> actual = new HashSet<String>();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add(page.get());
        }
        assertEquals(expected, actual);
    }

    //Test for AMQ-6590
    public void testFreePageRecoveryUncleanShutdown() throws Exception {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(false);
        pf.load();

        //Allocate 10 free pages
        Transaction tx = pf.tx();
        tx.allocate(10);
        tx.commit();
        pf.flush();

        //Load a second instance on the same directory fo the page file which
        //simulates an unclean shutdown from the previous run
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.setEnableRecoveryFile(false);
        pf2.load();
        try {
            assertTrue("We have 10 free pages", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    pf2.flush();
                    long freePages = pf2.getFreePageCount();
                    LOG.info("free page count: " + freePages);
                    return  freePages == 10l;
                }
            }, 100000));
        } finally {
            pf.unload();
            pf2.unload();
        }
    }

    public void testAllocatedAndUnusedAreFree() throws Exception {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        Transaction tx = pf.tx();
        tx.allocate(10);
        tx.commit();

        assertEquals(10, pf.getPageCount());
        assertEquals(pf.getFreePageCount(), 10);

        // free pages should get reused

        tx.allocate(10);
        tx.rollback();
        assertEquals(10, pf.getPageCount());
        assertEquals(pf.getFreePageCount(), 10);
    }

    //Test for AMQ-7080
    public void testFreePageRecoveryCleanShutdownAndRecoverFromDbFreeFile() throws Exception {

        int numberOfFreePages = 100;
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(true);
        pf.load();

        //Allocate free pages
        Page firstPage = pf.allocate(numberOfFreePages * 2);
        for (int i = 0; i < numberOfFreePages; i++) {
            pf.freePage(firstPage.pageId + i);
        }
        pf.flush();

        assertEquals(pf.getFreePageCount(), numberOfFreePages);

        // Reload it... clean shutdown
        pf.unload();
        pf.load();

        assertEquals(pf.getFreePageCount(), numberOfFreePages);
        pf.unload();
    }

    //Test for AMQ-7080
    public void testFreePageRecoveryUncleanShutdownAndRecoverFromDbFreeFile() throws Exception {
        AtomicBoolean asyncRecovery = verifyAsyncRecovery();
        int numberOfFreePages = 100;
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(true);
        pf.load();

        //Allocate free pages
        Transaction tx = pf.tx();
        tx.allocate(numberOfFreePages);
        tx.commit();

        pf.flush();

        assertEquals(pf.getFreePageCount(), numberOfFreePages);

        waitFreePageFileFlush(pf);

        //Load a second instance on the same directory fo the page file which
        //simulates an unclean shutdown from the previous run
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.load();

        assertEquals(pf.getFreePageCount(), pf2.getFreePageCount());

        pf2.unload();
        pf.unload();
        assertFalse(asyncRecovery.get());
    }

    //Test for AMQ-7080
    public void testFreePageRecoveryUncleanShutdownAndDirtyDbFree() throws Exception {
        AtomicBoolean asyncRecovery = verifyAsyncRecovery();
        int numberOfFreePages = 100;
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(true);
        pf.load();

        //Allocate free pages
        Transaction tx = pf.tx();
        tx.allocate(numberOfFreePages);
        tx.commit();
        pf.flush();

        FileInputStream is = new FileInputStream(pf.getFreeFile());
        DataInputStream dis = new DataInputStream(is);

        SequenceSet freeList = SequenceSet.Marshaller.INSTANCE.readPayload(dis);

        // Simulate outdated db.free
        freeList.getMetadata().setNextTxid(freeList.getMetadata().getNextTxid() - 1);
        freeList.remove(100);
        FileOutputStream os = new FileOutputStream(pf.getFreeFile());
        DataOutputStream dos = new DataOutputStream(os);
        SequenceSet.Marshaller.INSTANCE.writePayload(freeList, dos);

        //Load a second instance on the same directory fo the page file which
        //simulates an unclean shutdown from the previous run
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.load();

        try {
            assertTrue("We have 100 free pages", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {

                    pf2.flush();
                    long freePages = pf2.getFreePageCount();
                    LOG.info("free page count: " + freePages);
                    return  freePages == numberOfFreePages;
                }
            }, 100000));
        } finally {
            pf.unload();
            pf2.unload();
        }

        assertTrue(asyncRecovery.get());
    }

    //Test for AMQ-7080
    public void testFreePageRecoveryUncleanShutdownAndPartialWriteDbFree() throws Exception {
        AtomicBoolean asyncRecovery = verifyAsyncRecovery();
        int numberOfFreePages = 100;
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(true);
        pf.load();

        //Allocate free pages
        Transaction tx = pf.tx();
        tx.allocate(numberOfFreePages);
        tx.commit();
        pf.flush();

        waitFreePageFileFlush(pf);
        // Simulate a partial write
        RecoverableRandomAccessFile freeFile = new RecoverableRandomAccessFile(pf.getFreeFile(), "rw");
        freeFile.seek(10);
        freeFile.writeLong(170);
        freeFile.sync();

        //Load a second instance on the same directory fo the page file which
        //simulates an unclean shutdown from the previous run
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.load();

        try {
            assertTrue("We have 200 free pages", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {

                    pf2.flush();
                    long freePages = pf2.getFreePageCount();
                    LOG.info("free page count: " + freePages);
                    return  freePages == numberOfFreePages;
                }
            }, 100000));
        } finally {
            pf.unload();
            pf2.unload();
        }
        assertTrue(asyncRecovery.get());
    }

    //Test for AMQ-7080
    public void testFreePageRecoveryUncleanShutdownAndWrongFreePageSize() throws Exception {
        AtomicBoolean asyncRecovery = verifyAsyncRecovery();
        int numberOfFreePages = 100;
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(true);
        pf.load();

        //Allocate free pages
        Transaction tx = pf.tx();
        tx.allocate(numberOfFreePages);
        tx.commit();
        pf.flush();

        waitFreePageFileFlush(pf);
        // Simulate a partial write with free count bigger than what is written
        RecoverableRandomAccessFile freeFile = new RecoverableRandomAccessFile(pf.getFreeFile(), "rw");
        freeFile.seek(0);
        freeFile.writeInt(1000000);
        freeFile.sync();

        //Load a second instance on the same directory fo the page file which
        //simulates an unclean shutdown from the previous run
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.load();

        try {
            assertTrue("We have 200 free pages", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {

                    pf2.flush();
                    long freePages = pf2.getFreePageCount();
                    LOG.info("free page count: " + freePages);
                    return  freePages == numberOfFreePages;
                }
            }, 100000));
        } finally {
            pf.unload();
            pf2.unload();
        }
        assertTrue(asyncRecovery.get());
    }

    private void waitFreePageFileFlush(final PageFile pf) throws Exception {
        assertTrue("Free Page file saved", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                try {
                    FileInputStream is = new FileInputStream(pf.getFreeFile());
                    DataInputStream dis = new DataInputStream(is);
                    SequenceSet freeList = SequenceSet.Marshaller.INSTANCE.readPayload(dis);
                    return freeList.getMetadata().getNextTxid() == pf.getFreePageList().getMetadata().getNextTxid();
                } catch (Exception ex) {
                    // This file is written async
                    return false;
                }
            }
        }, 10000));
    }

    private AtomicBoolean verifyAsyncRecovery() {
        AtomicBoolean asyncRecovery = new AtomicBoolean(false);

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.INFO) && event.getMessage().toString().contains("Recovering pageFile free list")) {
                    asyncRecovery.set(true);
                }
            }
        };

        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(PageFile.class);
        log4jLogger.addAppender(appender);
        log4jLogger.setLevel(Level.DEBUG);
        return asyncRecovery;
    }
}
