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
package org.apache.activemq.store.kahadb.plist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.util.IOHelper;
import org.apache.kahadb.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PListTest {

    private PListStore store;
    private PList plist;
   

    @Test
    public void testAddLast() throws Exception {
        final int COUNT = 1000;
        Map<String, ByteSequence> map = new LinkedHashMap<String, ByteSequence>();
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            ByteSequence bs = new ByteSequence(test.getBytes());
            map.put(test, bs);
            plist.addLast(test, bs);
        }
        assertEquals(plist.size(), COUNT);
        int count = 0;
        for (ByteSequence bs : map.values()) {
            String origStr = new String(bs.getData(), bs.getOffset(), bs.getLength());
            PListEntry entry = plist.get(count);
            String plistString = new String(entry.getByteSequence().getData(), entry.getByteSequence().getOffset(),
                    entry.getByteSequence().getLength());
            assertEquals(origStr, plistString);
            count++;
        }
    }

   @Test
    public void testAddFirst() throws Exception {
        final int COUNT = 1000;
        Map<String, ByteSequence> map = new LinkedHashMap<String, ByteSequence>();
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            ByteSequence bs = new ByteSequence(test.getBytes());
            map.put(test, bs);
            plist.addFirst(test, bs);
        }
        assertEquals(plist.size(), COUNT);
        int count = plist.size() - 1;
        for (ByteSequence bs : map.values()) {
            String origStr = new String(bs.getData(), bs.getOffset(), bs.getLength());
            PListEntry entry = plist.get(count);
            String plistString = new String(entry.getByteSequence().getData(), entry.getByteSequence().getOffset(),
                    entry.getByteSequence().getLength());
            assertEquals(origStr, plistString);
            count--;
        }
    }

    @Test
    public void testRemove() throws IOException {
        doTestRemove(2000);
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
        PListEntry entry = plist.getFirst();
        while (entry != null) {
            plist.remove(entry.copy());
            entry = plist.getFirst();
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
        assertNull("no first entry", plist.getFirst());
    }
    
    @Test
    public void testRemoveSecond() throws Exception {
        plist.addLast("First", new ByteSequence("A".getBytes()));
        plist.addLast("Second", new ByteSequence("B".getBytes()));
        
        assertTrue(plist.remove("Second"));
        assertTrue(plist.remove("First"));
        assertFalse(plist.remove("doesNotExist"));
    }
    
    
    @Test
    public void testRemoveSecondPosition() throws Exception {
        plist.addLast("First", new ByteSequence("A".getBytes()));
        plist.addLast("Second", new ByteSequence("B".getBytes()));
        
        assertTrue(plist.remove(1));
        assertTrue(plist.remove(0));
        assertFalse(plist.remove(3));
    }


    @Test
    public void testConcurrentAddRemove() throws Exception {
        File directory = store.getDirectory();
        store.stop();
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        store = new PListStore();
        store.setDirectory(directory);
        store.setJournalMaxFileLength(1024*5);
        store.start();

        final ByteSequence payload = new ByteSequence(new byte[1024*4]);


        final Vector<Throwable> exceptions = new Vector<Throwable>();
        final int iterations = 1000;
        final int numLists = 10;

        final PList[] lists = new PList[numLists];
        for (int i=0; i<numLists; i++) {
            lists[i] = store.getPList("List" + i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(100);
        class A implements Runnable {
            @Override
            public void run() {
                try {
                    for (int i=0; i<iterations; i++) {
                        PList candidate = lists[i%numLists];
                        candidate.addLast(String.valueOf(i), payload);
                        PListEntry entry = candidate.getFirst();
                        assertTrue(candidate.remove(String.valueOf(i)));
                    }
                } catch (Exception error) {
                    error.printStackTrace();
                    exceptions.add(error);
                }
            }
        };

        class B implements  Runnable {
            @Override
            public void run() {
                try {
                    for (int i=0; i<iterations; i++) {
                        PList candidate = lists[i%numLists];
                        candidate.addLast(String.valueOf(i), payload);
                        PListEntry entry = candidate.getFirst();
                        assertTrue(candidate.remove(String.valueOf(i)));
                    }
                } catch (Exception error) {
                    error.printStackTrace();
                    exceptions.add(error);
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
        executor.awaitTermination(30, TimeUnit.SECONDS);

        assertTrue("no exceptions", exceptions.isEmpty());
    }

    @Before
    public void setUp() throws Exception {
        File directory = new File("target/test/PlistDB");
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        startStore(directory);

    }

    protected void startStore(File directory) throws Exception {
        store = new PListStore();
        store.setDirectory(directory);
        store.start();
        plist = store.getPList("test");
    }

    @After
    public void tearDown() throws Exception {
        store.stop();
    }

}
