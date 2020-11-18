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
package org.apache.activemq.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PListTestSupport {
    static final Logger LOG = LoggerFactory.getLogger(PListTestSupport.class);
    protected PListStore store;
    private PList plist;
    final ByteSequence payload = new ByteSequence(new byte[400]);
    final String idSeed = new String("Seed" + new byte[1024]);
    final Vector<Throwable> exceptions = new Vector<Throwable>();
    ExecutorService executor;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testAddLast() throws Exception {
        final int COUNT = 1000;
        LinkedList<ByteSequence> list = new LinkedList<ByteSequence>();
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            ByteSequence bs = new ByteSequence(test.getBytes());
            list.addLast(bs);
            plist.addLast(test, bs);
        }
        assertEquals(plist.size(), COUNT);

        PList.PListIterator actual = plist.iterator();
        Iterator<ByteSequence> expected = list.iterator();
        while (expected.hasNext()) {
            ByteSequence bs = expected.next();
            assertTrue(actual.hasNext());
            PListEntry entry = actual.next();
            String origStr = new String(bs.getData(), bs.getOffset(), bs.getLength());
            String plistString = new String(entry.getByteSequence().getData(), entry.getByteSequence().getOffset(),
                    entry.getByteSequence().getLength());
            assertEquals(origStr, plistString);
        }
        assertFalse(actual.hasNext());
    }

   @Test
    public void testAddFirst() throws Exception {
       final int COUNT = 1000;
       LinkedList<ByteSequence> list = new LinkedList<ByteSequence>();
       for (int i = 0; i < COUNT; i++) {
           String test = new String("test" + i);
           ByteSequence bs = new ByteSequence(test.getBytes());
           list.addFirst(bs);
           plist.addFirst(test, bs);
       }
       assertEquals(plist.size(), COUNT);

       PList.PListIterator actual = plist.iterator();
       Iterator<ByteSequence> expected = list.iterator();
       while (expected.hasNext()) {
           ByteSequence bs = expected.next();
           assertTrue(actual.hasNext());
           PListEntry entry = actual.next();
           String origStr = new String(bs.getData(), bs.getOffset(), bs.getLength());
           String plistString = new String(entry.getByteSequence().getData(), entry.getByteSequence().getOffset(),
                   entry.getByteSequence().getLength());
           assertEquals(origStr, plistString);
       }
       assertFalse(actual.hasNext());
   }

    @Test
    public void testRemove() throws IOException {
        doTestRemove(2000);
    }

    private PListEntry getFirst(PList plist) throws IOException {
        PList.PListIterator iterator = plist.iterator();
        try {
            if( iterator.hasNext() ) {
                return iterator.next();
            } else {
                return null;
            }
        }finally {
            iterator.release();
        }
    }

    protected void doTestRemove(final int COUNT) throws IOException {
        Map<String, ByteSequence> map = new LinkedHashMap<String, ByteSequence>();
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            ByteSequence bs = new ByteSequence(test.getBytes());
            map.put(test, bs);
            plist.addLast(test, bs);
        }
        assertEquals(plist.size(), COUNT);
        PListEntry entry = getFirst(plist);
        while (entry != null) {
            plist.remove(entry.getLocator());
            entry = getFirst(plist);
        }
        assertEquals(0,plist.size());

    }

    @Test
    public void testDestroy() throws Exception {
        doTestRemove(1);
        plist.destroy();
        assertEquals(0,plist.size());
    }

    @Test
    public void testDestroyNonEmpty() throws Exception {
        final int COUNT = 1000;
        Map<String, ByteSequence> map = new LinkedHashMap<String, ByteSequence>();
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            ByteSequence bs = new ByteSequence(test.getBytes());
            map.put(test, bs);
            plist.addLast(test, bs);
        }
        plist.destroy();
        assertEquals(0,plist.size());
    }

    @Test
    public void testRemoveSecond() throws Exception {
        Object first = plist.addLast("First", new ByteSequence("A".getBytes()));
        Object second = plist.addLast("Second", new ByteSequence("B".getBytes()));

        assertTrue(plist.remove(second));
        assertTrue(plist.remove(first));
        assertFalse(plist.remove(first));
    }

    @Test
    public void testRemoveSingleEntry() throws Exception {
        plist.addLast("First", new ByteSequence("A".getBytes()));

        Iterator<PListEntry> iterator = plist.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void testRemoveSecondPosition() throws Exception {
        Object first = plist.addLast("First", new ByteSequence("A".getBytes()));
        Object second = plist.addLast("Second", new ByteSequence("B".getBytes()));

        assertTrue(plist.remove(second));
        assertTrue(plist.remove(first));
        assertFalse(plist.remove(first));
    }

    @Test
    public void testConcurrentAddRemove() throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = createConcurrentAddRemovePListStore();
        store.setDirectory(directory);
        store.start();

        final ByteSequence payload = new ByteSequence(new byte[1024*2]);


        final Vector<Throwable> exceptions = new Vector<Throwable>();
        final int iterations = 1000;
        final int numLists = 10;

        final PList[] lists = new PList[numLists];
        String threadName = Thread.currentThread().getName();
        for (int i=0; i<numLists; i++) {
            Thread.currentThread().setName("C:"+String.valueOf(i));
            lists[i] = store.getPList(String.valueOf(i));
        }
        Thread.currentThread().setName(threadName);

        executor = Executors.newFixedThreadPool(100);
        class A implements Runnable {
            @Override
            public void run() {
                final String threadName = Thread.currentThread().getName();
                try {
                    for (int i=0; i<iterations; i++) {
                        PList candidate = lists[i%numLists];
                        Thread.currentThread().setName("ALRF:"+candidate.getName());
                        synchronized (plistLocks(candidate)) {
                            Object last = candidate.addLast(String.valueOf(i), payload);
                            getFirst(candidate);
                            assertTrue(candidate.remove(last));
                        }
                    }
                } catch (Exception error) {
                    LOG.error("Unexpcted ex", error);
                    error.printStackTrace();
                    exceptions.add(error);
                }  finally {
                    Thread.currentThread().setName(threadName);
                }
            }
        };

        class B implements  Runnable {
            @Override
            public void run() {
                final String threadName = Thread.currentThread().getName();
                try {
                    for (int i=0; i<iterations; i++) {
                        PList candidate = lists[i%numLists];
                        Thread.currentThread().setName("ALRF:"+candidate.getName());
                         synchronized (plistLocks(candidate)) {
                             Object last = candidate.addLast(String.valueOf(i), payload);
                             getFirst(candidate);
                             assertTrue(candidate.remove(last));
                         }
                    }
                } catch (Exception error) {
                    error.printStackTrace();
                    exceptions.add(error);
                }  finally {
                    Thread.currentThread().setName(threadName);
                }
            }
        };

        executor.execute(new A());
        executor.execute(new A());
        executor.execute(new A());
        executor.execute(new B());
        executor.execute(new B());
        executor.execute(new B());

        executor.shutdown();
        boolean finishedInTime = executor.awaitTermination(10, TimeUnit.MINUTES);
        LOG.info("Tested completion finished in time? -> {}", finishedInTime ? "YES" : "NO");

        assertTrue("no exceptions", exceptions.isEmpty());
        assertTrue("finished ok", finishedInTime);
    }

    protected abstract PListStore createConcurrentAddRemovePListStore();

    @Test
    public void testConcurrentAddLast() throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = createPListStore();
        store.setDirectory(directory);
        store.start();


        final int numThreads = 20;
        final int iterations = 1000;
        executor = Executors.newFixedThreadPool(100);
        for (int i=0; i<numThreads; i++) {
            new Job(i, PListTestSupport.TaskType.ADD, iterations).run();
        }

        for (int i=0; i<numThreads; i++) {
            executor.execute(new Job(i, PListTestSupport.TaskType.ITERATE, iterations));
        }

        for (int i=0; i<100; i++) {
            executor.execute(new Job(i+20, PListTestSupport.TaskType.ADD, 100));
        }

        executor.shutdown();
        boolean finishedInTime = executor.awaitTermination(60*5, TimeUnit.SECONDS);
        assertTrue("finished ok", finishedInTime);
    }

    @Test
    public void testOverFlow() throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = createPListStore();
        store.setDirectory(directory);
        store.start();

        for (int i=0;i<2000; i++) {
            new Job(i, PListTestSupport.TaskType.ADD, 5).run();

        }
//        LOG.info("After Load index file: " + store.pageFile.getFile().length());
//        LOG.info("After remove index file: " + store.pageFile.getFile().length());
    }

    @Test
    public void testConcurrentAddRemoveWithPreload() throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = createConcurrentAddRemoveWithPreloadPListStore();
        store.setDirectory(directory);
        store.start();

        final int iterations = 500;
        final int numLists = 10;

        // prime the store

        // create/delete
        LOG.info("create");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.CREATE, iterations).run();
        }

        LOG.info("delete");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.DELETE, iterations).run();
        }

        LOG.info("fill");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.ADD, iterations).run();
        }
        LOG.info("remove");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.REMOVE, iterations).run();
        }

        LOG.info("check empty");
        for (int i=0; i<numLists;i++) {
            assertEquals("empty " + i, 0, store.getPList("List-" + i).size());
        }

        LOG.info("delete again");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.DELETE, iterations).run();
        }

        LOG.info("fill again");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.ADD, iterations).run();
        }

        LOG.info("parallel add and remove");
        executor = Executors.newFixedThreadPool(numLists*2);
        for (int i=0; i<numLists*2; i++) {
            executor.execute(new Job(i, i>=numLists ? PListTestSupport.TaskType.ADD : PListTestSupport.TaskType.REMOVE, iterations));
        }

        executor.shutdown();
        LOG.info("wait for parallel work to complete");
        boolean finishedInTime = executor.awaitTermination(60*5, TimeUnit.SECONDS);
        assertTrue("no exceptions", exceptions.isEmpty());
        assertTrue("finished ok", finishedInTime);
    }

    protected abstract PListStore createConcurrentAddRemoveWithPreloadPListStore();

    // for non determinant issues, increasing this may help diagnose
    final int numRepeats = 1;

    @Test
    public void testRepeatStressWithCache() throws Exception {
        for (int i=0; i<numRepeats;i++) {
            do_testConcurrentAddIterateRemove(true);
        }
    }

    @Test
    public void testRepeatStressWithOutCache() throws Exception {
        for (int i=0; i<numRepeats;i++) {
            do_testConcurrentAddIterateRemove(false);
        }
    }

    public void do_testConcurrentAddIterateRemove(boolean enablePageCache) throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = createConcurrentAddIterateRemovePListStore(enablePageCache);
        store.setDirectory(directory);
        store.start();

        final int iterations = 500;
        final int numLists = 10;

        LOG.info("create");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.CREATE, iterations).run();
        }

        LOG.info("fill");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.ADD, iterations).run();
        }

        LOG.info("parallel add and remove");
        executor = Executors.newFixedThreadPool(400);
        final int numProducer = 5;
        final int numConsumer = 10;
        for (int i=0; i<numLists; i++) {
            for (int j=0; j<numProducer; j++) {
                executor.execute(new Job(i, PListTestSupport.TaskType.ADD, iterations*2));
            }
            for (int k=0;k<numConsumer; k++) {
                executor.execute(new Job(i, TaskType.ITERATE_REMOVE, iterations/4));
            }
        }

         for (int i=numLists; i<numLists*10; i++) {
            executor.execute(new Job(i, PListTestSupport.TaskType.ADD, iterations));
         }

        executor.shutdown();
        LOG.info("wait for parallel work to complete");
        boolean shutdown = executor.awaitTermination(60*60, TimeUnit.SECONDS);
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
        assertTrue("test did not  timeout ", shutdown);
    }

    protected abstract PListStore createConcurrentAddIterateRemovePListStore(boolean enablePageCache);

    @Ignore("Takes too long.. might have broken it.")
    @Test
    public void testConcurrentAddIterate() throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = createConcurrentAddIteratePListStore();
        store.setDirectory(directory);
        store.start();

        final int iterations = 250;
        final int numLists = 10;

        LOG.info("create");
        for (int i=0; i<numLists;i++) {
            new Job(i, PListTestSupport.TaskType.CREATE, iterations).run();
        }

        LOG.info("parallel add and iterate");
        // We want a lot of adds occurring so that new free pages get created along
        // with overlapping seeks from the iterators so that we are likely to seek into
        // some bad area in the page file.
        executor = Executors.newFixedThreadPool(400);
        final int numProducer = 300;
        final int numConsumer = 100;
        for (int i=0; i<numLists; i++) {
            for (int j=0; j<numProducer; j++) {
                executor.execute(new Job(i, PListTestSupport.TaskType.ADD, iterations));
            }
            for (int k=0;k<numConsumer; k++) {
                executor.execute(new Job(i, TaskType.ITERATE, iterations*2));
            }
        }

        executor.shutdown();
        LOG.info("wait for parallel work to complete");
        boolean shutdown = executor.awaitTermination(60*60, TimeUnit.SECONDS);
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
        assertTrue("test did not  timeout ", shutdown);
//        LOG.info("Num dataFiles:" + store.getJournal().getFiles().size());
    }

    abstract protected PListStore createConcurrentAddIteratePListStore();

    enum TaskType {CREATE, DELETE, ADD, REMOVE, ITERATE, ITERATE_REMOVE}
    ConcurrentMap<String, Object> entries = new ConcurrentHashMap<String, Object>();

    class Job implements Runnable {

        int id;
        TaskType task;
        int iterations;

        public Job(int id, TaskType t, int iterations) {
            this.id = id;
            this.task = t;
            this.iterations = iterations;
        }

        @Override
        public void run() {
            final String threadName = Thread.currentThread().getName();
            try {
                PList plist = null;
                switch (task) {
                    case CREATE:
                        Thread.currentThread().setName("C:"+id);
                        plist = store.getPList(String.valueOf(id));
                        LOG.info("Job-" + id + ", CREATE");
                        break;
                    case DELETE:
                        Thread.currentThread().setName("D:"+id);
                        store.removePList(String.valueOf(id));
                        break;
                    case ADD:
                        Thread.currentThread().setName("A:"+id);
                        plist = store.getPList(String.valueOf(id));

                        for (int j = 0; j < iterations; j++) {
                            synchronized (plistLocks(plist)) {
                                if (exceptions.isEmpty()) {
                                    String key = "PL>" + id + idSeed + "-" + j;
                                    entries.put(key, plist.addLast(key, payload));
                                } else {
                                    break;
                                }
                            }
                        }

                        if (exceptions.isEmpty()) {
                            LOG.info("Job-" + id + ", Add, done: " + iterations);
                        }
                        break;
                    case REMOVE:
                        Thread.currentThread().setName("R:"+id);
                        plist = store.getPList(String.valueOf(id));
                        synchronized (plistLocks(plist)) {

                            for (int j = iterations -1; j >= 0; j--) {
                                String key = "PL>" + id + idSeed + "-" + j;
                                Object position = entries.remove(key);
                                if( position!=null ) {
                                    plist.remove(position);
                                }
                                if (j > 0 && j % (iterations / 2) == 0) {
                                    LOG.info("Job-" + id + " Done remove: " + j);
                                }
                            }
                        }
                        break;
                    case ITERATE:
                        Thread.currentThread().setName("I:"+id);
                        plist = store.getPList(String.valueOf(id));
                        int iterateCount = 0;
                        synchronized (plistLocks(plist)) {
                            if (exceptions.isEmpty()) {
                                Iterator<PListEntry> iterator = plist.iterator();
                                while (iterator.hasNext() && exceptions.isEmpty()) {
                                    iterator.next();
                                    iterateCount++;
                                }

                                //LOG.info("Job-" + id + " Done iterate: it=" + iterator + ", count:" + iterateCount + ", size:" + plist.size());
                                if (plist.size() != iterateCount) {
                                    System.err.println("Count Wrong: " + iterator);
                                }
                                assertEquals("iterate got all " + id + " iterator:" + iterator , plist.size(), iterateCount);
                            }
                        }
                        break;

                    case ITERATE_REMOVE:
                        Thread.currentThread().setName("IRM:"+id);
                        plist = store.getPList(String.valueOf(id));

                        int removeCount = 0;
                        synchronized (plistLocks(plist)) {

                            Iterator<PListEntry> removeIterator = plist.iterator();

                            while (removeIterator.hasNext()) {
                                removeIterator.next();
                                removeIterator.remove();
                                if (removeCount++ > iterations) {
                                    break;
                                }
                            }
                        }
                        LOG.info("Job-" + id + " Done remove: " + removeCount);
                        break;

                    default:
                }

            } catch (Exception e) {
                LOG.warn("Job["+id+"] caught exception: " + e.getMessage());
                e.printStackTrace();
                exceptions.add(e);
                if (executor != null) {
                    executor.shutdownNow();
                }
            } finally {
                Thread.currentThread().setName(threadName);
            }
        }
    }

    Map<PList, Object> locks = new HashMap<PList, Object>();
    private Object plistLocks(PList plist) {
        Object lock = null;
        synchronized (locks) {
            if (locks.containsKey(plist)) {
                lock = locks.get(plist);
             } else {
                lock = new Object();
                locks.put(plist, lock);
            }
        }
        return lock;
    }

    @Before
    public void setUp() throws Exception {
        File directory = tempFolder.newFolder();
        startStore(directory);
    }

    protected void startStore(File directory) throws Exception {
        store = createPListStore();
        store.setDirectory(directory);
        store.start();
        plist = store.getPList("main");
    }

    abstract protected PListStore createPListStore();

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
        store.stop();
        exceptions.clear();
    }

}
