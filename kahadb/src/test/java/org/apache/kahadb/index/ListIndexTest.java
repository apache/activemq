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
package org.apache.kahadb.index;

import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.StringMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListIndexTest extends IndexTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ListIndexTest.class);
    private NumberFormat nf;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        nf = NumberFormat.getIntegerInstance();
        nf.setMinimumIntegerDigits(6);
        nf.setGroupingUsed(false);
    }

    @Override
    protected Index<String, Long> createIndex() throws Exception {

        long id = tx.allocate().getPageId();
        tx.commit();

        ListIndex<String, Long> index = new ListIndex<String, Long>(pf, id);
        index.setKeyMarshaller(StringMarshaller.INSTANCE);
        index.setValueMarshaller(LongMarshaller.INSTANCE);

        return index;
    }

    public void testSize() throws Exception {
        createPageFileAndIndex(100);

        ListIndex<String, Long> listIndex = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        int count = 30;
        tx = pf.tx();
        doInsert(count);
        tx.commit();
        assertEquals("correct size", count, listIndex.size());

        tx = pf.tx();
        Iterator<Map.Entry<String, Long>> iterator = index.iterator(tx);
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
            assertEquals("correct size", --count, listIndex.size());
        }
        tx.commit();

        count = 30;
        tx = pf.tx();
        doInsert(count);
        tx.commit();
        assertEquals("correct size", count, listIndex.size());

        tx = pf.tx();
        listIndex.clear(tx);
        assertEquals("correct size", 0, listIndex.size());
        tx.commit();
    }

    public void testAddFirst() throws Exception {
        createPageFileAndIndex(100);

        ListIndex<String, Long> listIndex = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        tx = pf.tx();

        // put is add last
        doInsert(10);
        listIndex.addFirst(tx, key(10), (long) 10);
        listIndex.addFirst(tx, key(11), (long) 11);

        tx.commit();

        tx = pf.tx();
        int counter = 11;
        Iterator<Map.Entry<String, Long>> iterator = index.iterator(tx);
        assertEquals(key(counter), iterator.next().getKey());
        counter--;
        assertEquals(key(counter), iterator.next().getKey());
        counter--;
        int count = 0;
        while (iterator.hasNext() && count < counter) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) iterator.next();
            assertEquals(key(count), entry.getKey());
            assertEquals(count, (long) entry.getValue());
            count++;
        }
        tx.commit();
    }

    public void testPruning() throws Exception {
        createPageFileAndIndex(100);

        ListIndex<String, Long> index = ((ListIndex<String, Long>) this.index);

        this.index.load(tx);
        tx.commit();

        long pageCount = index.getPageFile().getPageCount();
        assertEquals(1, pageCount);

        long freePageCount = index.getPageFile().getFreePageCount();
        assertEquals("No free pages", 0, freePageCount);

        tx = pf.tx();
        doInsert(20);
        tx.commit();

        pageCount = index.getPageFile().getPageCount();
        LOG.info("page count: " + pageCount);
        assertTrue("used some pages", pageCount > 1);

        tx = pf.tx();
        // Remove the data.
        doRemove(20);

        tx.commit();

        freePageCount = index.getPageFile().getFreePageCount();

        LOG.info("FreePage count: " + freePageCount);
        assertTrue("Some free pages " + freePageCount, freePageCount > 0);


        LOG.info("add some more to use up free list");
        tx = pf.tx();
        doInsert(20);
        tx.commit();

        freePageCount = index.getPageFile().getFreePageCount();

        LOG.info("FreePage count: " + freePageCount);
        assertEquals("no free pages " + freePageCount, 0, freePageCount);
        assertEquals("Page count is static", pageCount,  index.getPageFile().getPageCount());

        this.index.unload(tx);
        tx.commit();
    }

    public void testIterationAddFirst() throws Exception {
        createPageFileAndIndex(100);
        ListIndex<String, Long> index = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        tx = pf.tx();
        final int entryCount = 200;
        // Insert in reverse order..
        doInsertReverse(entryCount);
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();

        int counter = 0;
        for (Iterator<Map.Entry<String, Long>> i = index.iterator(tx); i.hasNext(); ) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) i.next();
            assertEquals(key(counter), entry.getKey());
            assertEquals(counter, (long) entry.getValue());
            counter++;
        }
         assertEquals("We iterated over all entries", entryCount, counter);

        tx = pf.tx();
        // Remove the data.
        doRemove(entryCount);
        tx.commit();

        this.index.unload(tx);
        tx.commit();
    }


    public void testIteration() throws Exception {
        createPageFileAndIndex(100);
        ListIndex<String, Long> index = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        // Insert in reverse order..
        final int entryCount = 200;
        doInsert(entryCount);

        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();

        int counter = 0;
        for (Iterator<Map.Entry<String, Long>> i = index.iterator(tx); i.hasNext(); ) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) i.next();
            assertEquals(key(counter), entry.getKey());
            assertEquals(counter, (long) entry.getValue());
            counter++;
        }
        assertEquals("We iterated over all entries", entryCount, counter);

        this.index.unload(tx);
        tx.commit();
    }

    public void testRandomRemove() throws Exception {

        createPageFileAndIndex(4*1024);
        ListIndex<String, Long> index = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        final int count = 4000;
        doInsert(count);

        Random rand = new Random(System.currentTimeMillis());
        int i = 0, prev = 0;
        while (!index.isEmpty(tx)) {
            prev = i;
            i = rand.nextInt(count);
            try {
                index.remove(tx, key(i));
            } catch (Exception e) {
                e.printStackTrace();
                fail("unexpected exception on " + i + ", prev: " + prev + ", ex: " + e);
            }
        }
    }

    public void testRemovePattern() throws Exception {
        createPageFileAndIndex(100);
        ListIndex<String, Long> index = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        final int count = 4000;
        doInsert(count);

        index.remove(tx, key(3697));
        index.remove(tx, key(1566));
    }

    public void testLargeAppendRemoveTimed() throws Exception {
        createPageFileAndIndex(1024*4);
        ListIndex<String, Long> listIndex = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();
        final int COUNT = 50000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; i++) {
             listIndex.put(tx, key(i), (long) i);
             tx.commit();
        }
        LOG.info("Time to add " + COUNT + ": " + (System.currentTimeMillis() - start) + " mills");
        LOG.info("Page count: " + listIndex.getPageFile().getPageCount());

        start = System.currentTimeMillis();
        tx = pf.tx();
        int removeCount = 0;
        Iterator<Map.Entry<String, Long>> iterator = index.iterator(tx);
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
            removeCount++;
        }
        tx.commit();
        assertEquals("Removed all", COUNT, removeCount);
        LOG.info("Time to remove " + COUNT + ": " + (System.currentTimeMillis() - start) + " mills");
        LOG.info("Page count: " + listIndex.getPageFile().getPageCount());
        LOG.info("Page free count: " + listIndex.getPageFile().getFreePageCount());
    }

    void doInsertReverse(int count) throws Exception {
        for (int i = count - 1; i >= 0; i--) {
            ((ListIndex) index).addFirst(tx, key(i), (long) i);
            tx.commit();
        }
    }

    @Override
    protected String key(int i) {
        return "key:" + nf.format(i);
    }
}
