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

import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class PageFileTest {

    private static final Logger LOG = LoggerFactory.getLogger(PageFileTest.class);

    private static String getName() {
        return "PageFileTest";
    }


    private boolean recoveryFile;

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][] {
                {true},
                {false}
        });
    }

    public PageFileTest(boolean recoveryFile) {
        this.recoveryFile = recoveryFile;
    }

    @Test
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

    @Test
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

    @Test
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
    @Test
    public void testFreePageRecoveryUncleanShutdown() throws Exception {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        //Allocate 10 free pages
        Transaction tx = pf.tx();
        tx.allocate(10);
        tx.commit();
        pf.flush();

        //Load a second instance on the same directory fo the page file which
        //simulates an unclean shutdown from the previous run
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.setEnableRecoveryFile(recoveryFile);
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
            }, 12000000));
        } finally {
            pf.unload();
            pf2.unload();
        }
    }

    @Test
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

    @Test
    public void testBackgroundRecoveryIsThreadSafe() throws Exception {

        int numberOfPages = 100000;

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        Transaction tx = pf.tx();
        tx.allocate(numberOfPages);
        tx.commit();
        LOG.info("Number of free pages:" + pf.getFreePageCount());
        pf.flush();

        //Simulate an unclean shutdown
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.setEnableRecoveryFile(recoveryFile);
        pf2.load();

        //For recovery = false, pagefile should new pages as the freeList still not recovered
        //For recovery = true, pagefile should use the recovered pages
        Transaction tx2 = pf2.tx();
        tx2.allocate(numberOfPages);
        tx2.commit();
        LOG.info("Number of free pages:" + pf2.getFreePageCount());
        assertEquals(pf2.getPageCount(), recoveryFile ? numberOfPages : 2 * numberOfPages);

        List<Transaction> transactions = new LinkedList<>();

        Thread.sleep(500);
        LOG.info("Creating Transactions");
        for (int i = 0; i < 20; i++) {
            Transaction txConcurrent = pf2.tx();
            Page page = txConcurrent.allocate();
            String t = "page:" + i;
            page.set(t);
            txConcurrent.store(page, StringMarshaller.INSTANCE, false);
            txConcurrent.commit();
            transactions.add(txConcurrent);
            Thread.sleep(50);
        }

        assertTrue("We have 199980 free pages", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                pf2.flush();
                long freePages = pf2.getFreePageCount();
                LOG.info("free page count: " + freePages);
                return  freePages == (recoveryFile ? numberOfPages - 20 : 2 * numberOfPages - 20);
            }
        }, 12000000));

        for (Transaction txConcurrent2: transactions) {
            for (Page page : txConcurrent2) {
                assertFalse(pf2.isFreePage(page.pageId));
            }
        }

    }

    @Test
    public void testBackgroundWillNotMarkEaslyPagesAsFree() throws Exception {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        Transaction tx = pf.tx();
        tx.allocate(100000);
        tx.commit();
        LOG.info("Number of free pages:" + pf.getFreePageCount());
        pf.flush();

        //Simulate an unclean shutdown
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.setEnableRecoveryFile(recoveryFile);
        pf2.load();

        //For recovery = false, pagefile should new pages as the freeList still not recovered
        //For recovery = true, pagefile should use the recovered pages
        Transaction tx2 = pf2.tx();
        tx2.allocate(200);
        tx2.commit();
        LOG.info("Number of free pages:" + pf2.getFreePageCount());
        assertEquals(pf2.getPageCount(), recoveryFile ? 100000 : 100200);

        Transaction tx3 = pf2.tx();
        tx3.allocate(100);

        assertTrue("We have 10 free pages", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                pf2.flush();
                long freePages = pf2.getFreePageCount();
                LOG.info("free page count: " + freePages);
                return  freePages == (recoveryFile ? 99900 : 100100);
            }
        }, 12000000));
    }

    public void testBackgroundWillMarkUsedPagesAsFreeInTheBeginning() throws Exception {
        final int numberOfPages = 100000;
        final AtomicBoolean recoveryEnd = new AtomicBoolean();

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.INFO) && event.getMessage().toString().contains("Recovered pageFile free list")) {
                    recoveryEnd.set(true);
                }
            }
        };

        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(PageFile.class);
        log4jLogger.addAppender(appender);
        log4jLogger.setLevel(Level.DEBUG);

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        List<Long> pagesToFree = new LinkedList<>();

        LOG.info("Creating Transactions");
        for (int i = 0; i < numberOfPages; i++) {
            Transaction tx = pf.tx();
            Page page = tx.allocate();
            String t = "page:" + i;
            page.set(t);
            tx.store(page, StringMarshaller.INSTANCE, false);
            tx.commit();

            if (i >= numberOfPages / 2) {
                pagesToFree.add(page.getPageId());
            }
        }

        pf.flush();

        LOG.info("Number of free pages:" + pf.getFreePageCount());

        //Simulate an unclean shutdown
        final PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.setEnableRecoveryFile(recoveryFile);
        pf2.load();

        LOG.info("RecoveredPageFile: Number of free pages:" + pf2.getFreePageCount());

        Transaction tx = pf2.tx();

        for (Long pageId : pagesToFree) {
            tx.free(tx.load(pageId,  StringMarshaller.INSTANCE));
            tx.commit();
        }

        LOG.info("RecoveredPageFile: Number of free pages Before Reusing:" + pf2.getFreePageCount());
        List<Transaction> transactions = new LinkedList<>();

        int totalFreePages = numberOfPages / 2;
        int totalPages = numberOfPages;

        for (int i = 0; i < 20; i++) {
            tx = pf2.tx();
            Page page = tx.allocate();
            String t = "page:" + i;
            page.set(t);
            tx.store(page, StringMarshaller.INSTANCE, false);
            tx.commit();
            transactions.add(tx);

            // Free pages was already recovered
            if (page.getPageId() < numberOfPages) {
                totalFreePages--;
            } else {
                totalPages++;
            }
        }

        LOG.info("RecoveredPageFile: Number of free pages After Reusing:" + pf2.getFreePageCount());

        assertTrue("Recovery Finished", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                pf2.flush();
                long freePages = pf2.getFreePageCount();
                LOG.info("free page count: " + freePages);
                return  recoveryEnd.get();
            }
        }, 100000));

        LOG.info("RecoveredPageFile: Number of free pages:" + pf2.getFreePageCount());

        for (Transaction txConcurrent: transactions) {
            for (Page page : txConcurrent) {
                assertFalse(pf2.isFreePage(page.pageId));
            }
        }

        // Make sure we dont have leaking pages.
        assertEquals(pf2.getFreePageCount(), totalFreePages);
        assertEquals(pf2.getPageCount(), totalPages);

        assertEquals("pages freed during recovery should be reused", numberOfPages, totalPages);
    }

    @Test
    public void shouldNotSaveFreePageMapWhenBackgroundRecoveryStillGoing() throws Exception {
        int numberOfTransactions = 40000;
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        LOG.info("Creating Transactions");
        for (int i = 0; i < numberOfTransactions; i++) {
            Transaction transaction = pf.tx();
            Page page = transaction.allocate();
            String t = "page:" + i;
            page.set(t);
            transaction.store(page, StringMarshaller.INSTANCE, false);
            transaction.commit();
        }

        assertEquals(0, pf.getFreePageCount());
        assertEquals(numberOfTransactions, pf.getPageCount());

        pf.flush();

        //Unclean shutdown and force background recovery
        PageFile pf2 = new PageFile(new File("target/test-data"), getName());
        pf2.getFreeFileMap().delete();
        pf2.setEnableRecoveryFile(recoveryFile);
        pf2.load();

        for (int i = 0; i < 100; i++) {
            Transaction transaction = pf2.tx();
            transaction.free(i * 4);
            transaction.commit();
        }

        pf2.flush();

        // Should not save the map file during background srecovery
        assertEquals(0, pf2.getFreeFileMap().length());

        assertTrue("Free Page map was written", Wait.waitFor(() -> {
            pf2.flush();
            return  pf2.getFreeFileMap().length() != 0;
        }, 12000000));

        pf2.unload();

        //Clean Shurdown
        PageFile pf3 = new PageFile(new File("target/test-data"), getName());
        pf3.setEnableRecoveryFile(recoveryFile);
        pf3.load();

        assertEquals(100, pf3.getFreePageCount());
    }

    @Test
    public void testFreePageRecoveryCleanShutdown() throws Exception {
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        LOG.info("Creating Transactions");
        for (int i = 0; i < 400; i++) {
            Transaction transaction = pf.tx();
            Page page = transaction.allocate();
            String t = "page:" + i;
            page.set(t);
            transaction.store(page, StringMarshaller.INSTANCE, false);
            transaction.commit();
        }

        assertEquals(0, pf.getFreePageCount());
        assertEquals(400, pf.getPageCount());

        pf.unload();

        // Clear Shutdown
        pf = new PageFile(new File("target/test-data"), getName());
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        assertEquals(0, pf.getFreePageCount());

        for (int i = 0; i < 100; i++) {
            Transaction transaction = pf.tx();
            transaction.free(i * 4);
            transaction.commit();;
        }

        assertEquals(100, pf.getFreePageCount());
        assertEquals(400, pf.getPageCount());

        pf.unload();

        // Clear Shutdown
        pf = new PageFile(new File("target/test-data"), getName());
        pf.setEnableRecoveryFile(recoveryFile);
        pf.load();

        assertEquals(100, pf.getFreePageCount());
        assertEquals(400, pf.getPageCount());

        for (int i = 0; i < 400; i++) {
            assertEquals(pf.isFreePage(i), i % 4 == 0);
        }

        pf.unload();
    }

}
