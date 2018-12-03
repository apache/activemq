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
package org.apache.activemq.store.kahadb.disk.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.util.LongMarshaller;
import org.apache.activemq.store.kahadb.disk.util.Sequence;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.junit.Test;
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
        ListIndex<String, Long> index = new ListIndex<String, Long>(pf, id);
        index.setKeyMarshaller(StringMarshaller.INSTANCE);
        index.setValueMarshaller(LongMarshaller.INSTANCE);
        index.load(tx);
        tx.commit();
        return index;
    }

    @Test(timeout=60000)
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

    @Test(timeout=60000)
    public void testPut() throws Exception {
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
        Long value = listIndex.get(tx, key(10));
        assertNotNull(value);
        listIndex.put(tx, key(10), Long.valueOf(1024));
        tx.commit();

        tx = pf.tx();
        value = listIndex.get(tx, key(10));
        assertEquals(1024L, value.longValue());
        assertTrue(listIndex.size() == 30);
        tx.commit();

        tx = pf.tx();
        value = listIndex.put(tx, key(31), Long.valueOf(2048));
        assertNull(value);
        assertTrue(listIndex.size() == 31);
        tx.commit();
    }

    @Test(timeout=60000)
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
            Map.Entry<String, Long> entry = iterator.next();
            assertEquals(key(count), entry.getKey());
            assertEquals(count, (long) entry.getValue());
            count++;
        }
        tx.commit();
    }

    @Test(timeout=60000)
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

    @Test(timeout=60000)
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
            Map.Entry<String, Long> entry = i.next();
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

    @Test(timeout=60000)
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
            Map.Entry<String, Long> entry = i.next();
            assertEquals(key(counter), entry.getKey());
            assertEquals(counter, (long) entry.getValue());
            counter++;
        }
        assertEquals("We iterated over all entries", entryCount, counter);

        this.index.unload(tx);
        tx.commit();
    }

    // https://issues.apache.org/jira/browse/AMQ-4221
    public void testIterationRemoveTailAndPrev() throws Exception {
        createPageFileAndIndex(100);
        ListIndex<String, Long> index = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();

        String payload = new String(new byte[8]);
        final int entryCount = 9;
        for (int i=0; i<entryCount; i++) {
            index.put(tx, payload + "-" + i, (long)i);
        }
        tx.commit();

        int counter = 0;
        long[] toRemove = new long[] {6, 7, 8};
        for (Iterator<Map.Entry<String, Long>> i = index.iterator(tx); i.hasNext(); ) {
            Map.Entry<String, Long> entry = i.next();
            assertEquals(counter, (long) entry.getValue());
            if (Arrays.binarySearch(toRemove, counter++)  >= 0) {
                i.remove();
            }
        }
        assertEquals("We iterated over all entries", entryCount, counter);

        this.index.unload(tx);
        tx.commit();
    }

    @Test(timeout=60000)
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

    @Test(timeout=60000)
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

    @Test(timeout=60000)
    public void testLargeAppendRemoveTimed() throws Exception {
        createPageFileAndIndex(1024*4);
        ListIndex<String, Long> listIndex = ((ListIndex<String, Long>) this.index);
        this.index.load(tx);
        tx.commit();
        final int COUNT = 50000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; i++) {
             listIndex.add(tx, key(i), (long) i);
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

    private int getMessageSize(int min, int max) {
        return min + (int)(Math.random() * ((max - min) + 1));
    }

    @Test(timeout=60000)
    public void testLargeValueOverflow() throws Exception {
        pf = new PageFile(getDirectory(), getClass().getName());
        pf.setPageSize(4*1024);
        pf.setEnablePageCaching(false);
        pf.setWriteBatchSize(1);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        ListIndex<Long, String> test = new ListIndex<Long, String>(pf, id);
        test.setKeyMarshaller(LongMarshaller.INSTANCE);
        test.setValueMarshaller(StringMarshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        final long NUM_ADDITIONS = 32L;

        LinkedList<Long> expected = new LinkedList<Long>();

        tx =  pf.tx();
        for (long i = 0; i < NUM_ADDITIONS; ++i) {
            final int stringSize = getMessageSize(1, 4096);
            String val = new String(new byte[stringSize]);
            expected.add(Long.valueOf(stringSize));
            test.add(tx, i, val);
        }
        tx.commit();

        tx =  pf.tx();
        for (long i = 0; i < NUM_ADDITIONS; i++) {
            String s = test.get(tx, i);
            assertEquals("string length did not match expected", expected.get((int)i), Long.valueOf(s.length()));
        }
        tx.commit();

        expected.clear();

        tx =  pf.tx();
        for (long i = 0; i < NUM_ADDITIONS; ++i) {
            final int stringSize = getMessageSize(1, 4096);
            String val = new String(new byte[stringSize]);
            expected.add(Long.valueOf(stringSize));
            test.addFirst(tx, i+NUM_ADDITIONS, val);
        }
        tx.commit();

        tx =  pf.tx();
        for (long i = 0; i < NUM_ADDITIONS; i++) {
            String s = test.get(tx, i+NUM_ADDITIONS);
            assertEquals("string length did not match expected", expected.get((int)i), Long.valueOf(s.length()));
        }
        tx.commit();

        expected.clear();

        tx =  pf.tx();
        for (long i = 0; i < NUM_ADDITIONS; ++i) {
            final int stringSize = getMessageSize(1, 4096);
            String val = new String(new byte[stringSize]);
            expected.add(Long.valueOf(stringSize));
            test.put(tx, i, val);
        }
        tx.commit();

        tx =  pf.tx();
        for (long i = 0; i < NUM_ADDITIONS; i++) {
            String s = test.get(tx, i);
            assertEquals("string length did not match expected", expected.get((int)i), Long.valueOf(s.length()));
        }
        tx.commit();
    }

    void doInsertReverse(int count) throws Exception {
        for (int i = count - 1; i >= 0; i--) {
            ((ListIndex<String, Long>) index).addFirst(tx, key(i), (long) i);
            tx.commit();
        }
    }

    @Override
    protected String key(int i) {
        return "key:" + nf.format(i);
    }

    @Test(timeout=60000)
    public void testListIndexConsistencyOverTime() throws Exception {

        final int NUM_ITERATIONS = 100;

        pf = new PageFile(getDirectory(), getClass().getName());
        pf.setPageSize(4*1024);
        pf.setEnablePageCaching(false);
        pf.setWriteBatchSize(1);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        ListIndex<String, SequenceSet> test = new ListIndex<String, SequenceSet>(pf, id);
        test.setKeyMarshaller(StringMarshaller.INSTANCE);
        test.setValueMarshaller(SequenceSet.Marshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        int expectedListEntries = 0;
        int nextSequenceId = 0;

        LOG.info("Loading up the ListIndex with "+NUM_ITERATIONS+" entires and sparsely populating the sequences.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            test.add(tx, String.valueOf(expectedListEntries++), new SequenceSet());

            for (int j = 0; j < expectedListEntries; j++) {

                SequenceSet sequenceSet = test.get(tx, String.valueOf(j));

                int startSequenceId = nextSequenceId;
                for (int ix = 0; ix < NUM_ITERATIONS; ix++) {
                    sequenceSet.add(nextSequenceId++);
                    test.put(tx, String.valueOf(j), sequenceSet);
                }

                sequenceSet = test.get(tx, String.valueOf(j));

                for (int ix = 0; ix < NUM_ITERATIONS - 1; ix++) {
                    sequenceSet.remove(startSequenceId++);
                    test.put(tx, String.valueOf(j), sequenceSet);
                }
            }
        }

        LOG.info("Checking if Index has the expected number of entries.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.get(tx, String.valueOf(i)));
        }

        LOG.info("Index has the expected number of entries.");

        assertEquals(expectedListEntries, test.size());

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            LOG.debug("Size of ListIndex before removal of entry ["+i+"] is: " + test.size());

            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.remove(tx, String.valueOf(i)));
            LOG.debug("Size of ListIndex after removal of entry ["+i+"] is: " + test.size());

            assertEquals(expectedListEntries - (i + 1), test.size());
        }
    }

    @Test(timeout=60000)
    public void testListLargeDataAddWithReverseRemove() throws Exception {

        final int NUM_ITERATIONS = 100;

        pf = new PageFile(getDirectory(), getClass().getName());
        pf.setPageSize(4*1024);
        pf.setEnablePageCaching(false);
        pf.setWriteBatchSize(1);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        ListIndex<String, SequenceSet> test = new ListIndex<String, SequenceSet>(pf, id);
        test.setKeyMarshaller(StringMarshaller.INSTANCE);
        test.setValueMarshaller(SequenceSet.Marshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        int expectedListEntries = 0;
        int nextSequenceId = 0;

        LOG.info("Loading up the ListIndex with "+NUM_ITERATIONS+" entries and sparsely populating the sequences.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            test.add(tx, String.valueOf(expectedListEntries++), new SequenceSet());

            for (int j = 0; j < expectedListEntries; j++) {

                SequenceSet sequenceSet = test.get(tx, String.valueOf(j));

                int startSequenceId = nextSequenceId;
                for (int ix = 0; ix < NUM_ITERATIONS; ix++) {
                    sequenceSet.add(nextSequenceId++);
                    test.put(tx, String.valueOf(j), sequenceSet);
                }

                sequenceSet = test.get(tx, String.valueOf(j));

                for (int ix = 0; ix < NUM_ITERATIONS - 1; ix++) {
                    sequenceSet.remove(startSequenceId++);
                    test.put(tx, String.valueOf(j), sequenceSet);
                }
            }
        }

        LOG.info("Checking if Index has the expected number of entries.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.get(tx, String.valueOf(i)));
        }

        LOG.info("Index has the expected number of entries.");

        assertEquals(expectedListEntries, test.size());

        for (int i = NUM_ITERATIONS - 1; i >= 0; --i) {
            LOG.debug("Size of ListIndex before removal of entry ["+i+"] is: " + test.size());

            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.remove(tx, String.valueOf(i)));
            LOG.debug("Size of ListIndex after removal of entry ["+i+"] is: " + test.size());

            assertEquals(--expectedListEntries, test.size());
        }
    }

    public void x_testSplitPerformance() throws Exception {

        final int NUM_ITERATIONS = 200;
        final int RANGE = 200000;

        pf = new PageFile(getDirectory(), getClass().getName());
        pf.setPageSize(4*1024);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        ListIndex<String, SequenceSet> test = new ListIndex<String, SequenceSet>(pf, id);
        test.setKeyMarshaller(StringMarshaller.INSTANCE);
        test.setValueMarshaller(SequenceSet.Marshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            Sequence sequence = new Sequence(0);
            sequence.setLast(RANGE);
            SequenceSet sequenceSet  = new SequenceSet();
            sequenceSet.add(sequence);
            test.add(tx, String.valueOf(i), sequenceSet);
        }

        long start = System.currentTimeMillis();

        // overflow the value in the last sequence
        SequenceSet sequenceSet = test.get(tx, String.valueOf(NUM_ITERATIONS - 10));
        for (int i=0; i<RANGE; i+=2) {
            sequenceSet.remove(i);
            test.put(tx, String.valueOf(NUM_ITERATIONS -1), sequenceSet);
        }
        LOG.info("duration: " + (System.currentTimeMillis() - start));
    }

    @Test(timeout=60000)
    public void testListLargeDataAddAndNonSequentialRemove() throws Exception {

        final int NUM_ITERATIONS = 100;

        pf = new PageFile(getDirectory(), getClass().getName());
        pf.setPageSize(4*1024);
        pf.setEnablePageCaching(false);
        pf.setWriteBatchSize(1);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        ListIndex<String, SequenceSet> test = new ListIndex<String, SequenceSet>(pf, id);
        test.setKeyMarshaller(StringMarshaller.INSTANCE);
        test.setValueMarshaller(SequenceSet.Marshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        int expectedListEntries = 0;
        int nextSequenceId = 0;

        LOG.info("Loading up the ListIndex with "+NUM_ITERATIONS+" entires and sparsely populating the sequences.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            test.add(tx, String.valueOf(expectedListEntries++), new SequenceSet());

            for (int j = 0; j < expectedListEntries; j++) {

                SequenceSet sequenceSet = test.get(tx, String.valueOf(j));

                int startSequenceId = nextSequenceId;
                for (int ix = 0; ix < NUM_ITERATIONS; ix++) {
                    sequenceSet.add(nextSequenceId++);
                    test.put(tx, String.valueOf(j), sequenceSet);
                }

                sequenceSet = test.get(tx, String.valueOf(j));

                for (int ix = 0; ix < NUM_ITERATIONS - 1; ix++) {
                    sequenceSet.remove(startSequenceId++);
                    test.put(tx, String.valueOf(j), sequenceSet);
                }
            }
        }

        LOG.info("Checking if Index has the expected number of entries.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.get(tx, String.valueOf(i)));
        }

        LOG.info("Index has the expected number of entries.");

        assertEquals(expectedListEntries, test.size());

        for (int i = 0; i < NUM_ITERATIONS; i += 2) {
            LOG.debug("Size of ListIndex before removal of entry ["+i+"] is: " + test.size());

            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.remove(tx, String.valueOf(i)));
            LOG.debug("Size of ListIndex after removal of entry ["+i+"] is: " + test.size());

            assertEquals(--expectedListEntries, test.size());
        }

        for (int i = NUM_ITERATIONS - 1; i > 0; i -= 2) {
            LOG.debug("Size of ListIndex before removal of entry ["+i+"] is: " + test.size());

            assertTrue("List should contain Key["+i+"]",test.containsKey(tx, String.valueOf(i)));
            assertNotNull("List contents of Key["+i+"] should not be null", test.remove(tx, String.valueOf(i)));
            LOG.debug("Size of ListIndex after removal of entry ["+i+"] is: " + test.size());

            assertEquals(--expectedListEntries, test.size());
        }

        assertEquals(0, test.size());
    }

    static class HashSetStringMarshaller extends VariableMarshaller<HashSet<String>> {
        final static HashSetStringMarshaller INSTANCE = new HashSetStringMarshaller();

        @Override
		public void writePayload(HashSet<String> object, DataOutput dataOut) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oout = new ObjectOutputStream(baos);
            oout.writeObject(object);
            oout.flush();
            oout.close();
            byte[] data = baos.toByteArray();
            dataOut.writeInt(data.length);
            dataOut.write(data);
        }

        @Override
		@SuppressWarnings("unchecked")
        public HashSet<String> readPayload(DataInput dataIn) throws IOException {
            int dataLen = dataIn.readInt();
            byte[] data = new byte[dataLen];
            dataIn.readFully(data);
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream oin = new ObjectInputStream(bais);
            try {
                return (HashSet<String>) oin.readObject();
            } catch (ClassNotFoundException cfe) {
                IOException ioe = new IOException("Failed to read HashSet<String>: " + cfe);
                ioe.initCause(cfe);
                throw ioe;
            }
        }
    }
}
