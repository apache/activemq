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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.StringMarshaller;
import org.apache.kahadb.util.VariableMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BTreeIndexTest extends IndexTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(BTreeIndexTest.class);
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

        BTreeIndex<String, Long> index = new BTreeIndex<String,Long>(pf, id);
        index.setKeyMarshaller(StringMarshaller.INSTANCE);
        index.setValueMarshaller(LongMarshaller.INSTANCE);
        
        return index;
    }

    /**
     * Yeah, the current implementation does NOT try to balance the tree.  Here is 
     * a test case showing that it gets out of balance.  
     * 
     * @throws Exception
     */
    public void disabled_testTreeBalancing() throws Exception {
        createPageFileAndIndex(100);

        BTreeIndex index = ((BTreeIndex)this.index);
        this.index.load(tx);
        tx.commit();
        
        doInsert(50);
        
        int minLeafDepth = index.getMinLeafDepth(tx);
        int maxLeafDepth = index.getMaxLeafDepth(tx);
        assertTrue("Tree is balanced", maxLeafDepth-minLeafDepth <= 1);

        // Remove some of the data
        doRemove(16);
        minLeafDepth = index.getMinLeafDepth(tx);
        maxLeafDepth = index.getMaxLeafDepth(tx);

        System.out.println( "min:"+minLeafDepth );
        System.out.println( "max:"+maxLeafDepth );
        index.printStructure(tx, new PrintWriter(System.out));

        assertTrue("Tree is balanced", maxLeafDepth-minLeafDepth <= 1);

        this.index.unload(tx);
    }
    
    public void testPruning() throws Exception {
        createPageFileAndIndex(100);

        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);

        this.index.load(tx);
        tx.commit();
     
        int minLeafDepth = index.getMinLeafDepth(tx);
        int maxLeafDepth = index.getMaxLeafDepth(tx);
        assertEquals(1, minLeafDepth);
        assertEquals(1, maxLeafDepth);
        
        doInsert(1000);
        
        minLeafDepth = index.getMinLeafDepth(tx);
        maxLeafDepth = index.getMaxLeafDepth(tx);
        assertTrue("Depth of tree grew", minLeafDepth > 1);
        assertTrue("Depth of tree grew", maxLeafDepth > 1);

        // Remove the data.
        doRemove(1000);
        minLeafDepth = index.getMinLeafDepth(tx);
        maxLeafDepth = index.getMaxLeafDepth(tx);

        assertEquals(1, minLeafDepth);
        assertEquals(1, maxLeafDepth);

        this.index.unload(tx);
        tx.commit();
    }

    public void testIteration() throws Exception {
        createPageFileAndIndex(500);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);
        tx.commit();
          
        // Insert in reverse order..
        doInsertReverse(1000);
        
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();

        exerciseAnotherIndex(tx);

        // BTree should iterate it in sorted order.
        int counter=0;
        for (Iterator<Map.Entry<String,Long>> i = index.iterator(tx); i.hasNext();) {
            Map.Entry<String,Long> entry = (Map.Entry<String,Long>)i.next();
            assertEquals(key(counter),entry.getKey());
            assertEquals(counter,(long)entry.getValue());
            counter++;
        }

        this.index.unload(tx);
        tx.commit();
    }
    
    
    public void testVisitor() throws Exception {
        createPageFileAndIndex(100);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);
        tx.commit();
          
        // Insert in reverse order..
        doInsert(1000);
        
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();

        // BTree should iterate it in sorted order.
        
        index.visit(tx, new BTreeVisitor<String, Long>(){
            public boolean isInterestedInKeysBetween(String first, String second) {
                return true;
            }
            public void visit(List<String> keys, List<Long> values) {
            }
        });
        

        this.index.unload(tx);
        tx.commit();
    }


    public void testRandomRemove() throws Exception {

        createPageFileAndIndex(100);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);

        long id = tx.allocate().getPageId();
        tx.commit();

        BTreeIndex<String, String> sindex = new BTreeIndex<String,String>(pf, id);
        sindex.setKeyMarshaller(StringMarshaller.INSTANCE);
        sindex.setValueMarshaller(StringMarshaller.INSTANCE);
        sindex.load(tx);

        tx.commit();

        final int count = 5000;

        String payload = new String(new byte[2]);
        for (int i = 0; i < count; i++) {
            index.put(tx, key(i), (long)i);
            sindex.put(tx, key(i), String.valueOf(i) + payload);
            tx.commit();
        }


        Random rand = new Random(System.currentTimeMillis());
        int i = 0, prev = 0;
        while (!index.isEmpty(tx) || !sindex.isEmpty(tx)) {
            prev = i;
            i = rand.nextInt(count);
            try {
                index.remove(tx, key(i));
                sindex.remove(tx, key(i));
            } catch (Exception e) {
                e.printStackTrace();
                fail("unexpected exception on " + i + ", prev: " + prev + ", ex: " + e);
            }
        }
    }

    public void testRandomAddRemove() throws Exception {

        createPageFileAndIndex(1024);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);

        long id = tx.allocate().getPageId();
        tx.commit();

        BTreeIndex<String, String> sindex = new BTreeIndex<String,String>(pf, id);
        sindex.setKeyMarshaller(StringMarshaller.INSTANCE);
        sindex.setValueMarshaller(StringMarshaller.INSTANCE);
        sindex.load(tx);

        tx.commit();

        Random rand = new Random(System.currentTimeMillis());
        final int count = 50000;

        String payload = new String(new byte[200]);
        for (int i = 0; i < count; i++) {
            int insertIndex = rand.nextInt(count);
            index.put(tx, key(insertIndex), (long)insertIndex);
            sindex.put(tx, key(insertIndex), String.valueOf(insertIndex) + payload);
            tx.commit();
        }


        int i = 0, prev = 0;
        while (!index.isEmpty(tx) || !sindex.isEmpty(tx)) {
            prev = i;
            i = rand.nextInt(count);
            try {
                index.remove(tx, key(i));
                sindex.remove(tx, key(i));
            } catch (Exception e) {
                e.printStackTrace();
                fail("unexpected exception on " + i + ", prev: " + prev + ", ex: " + e);
            }
        }
    }

    public void testRemovePattern() throws Exception {
        createPageFileAndIndex(100);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);
        tx.commit();

        final int count = 4000;
        doInsert(count);

        index.remove(tx, key(3697));
        index.remove(tx, key(1566));

        tx.commit();
        index.clear(tx);
        tx.commit();

        doInsert(count);

        Iterator<Map.Entry<String, Long>> iterator = index.iterator(tx, key(1345));
        while (iterator.hasNext()) {
            Map.Entry<String, Long> val = iterator.next();
        }

        doRemoveBackwards(666);
        Map.Entry<String, Long> first = index.getFirst(tx);
        assertEquals(first.getValue(), Long.valueOf(666L));

        for (int i=0; i<2000; i++) {
            Map.Entry<String, Long> last = index.getLast(tx);
            index.remove(tx, last.getKey());
            tx.commit();
        }

        exerciseAnotherIndex(tx);

        iterator = index.iterator(tx, key(100));
        while (iterator.hasNext()) {
            Map.Entry<String, Long> val = iterator.next();
        }

        Map.Entry<String, Long> last = index.getLast(tx);
        assertEquals(last.getValue(), Long.valueOf(1999L));
        index.clear(tx);
        assertNull(index.getLast(tx));
    }

    public void testLargeValue() throws Exception {
        //System.setProperty("maxKahaDBTxSize", "" + (1024*1024*1024));
        pf = new PageFile(directory, getClass().getName());
        pf.setPageSize(4*1024);
        //pf.setEnablePageCaching(false);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();
        tx.commit();

        BTreeIndex<Long, HashSet<String>> test = new BTreeIndex<Long, HashSet<String>>(pf, id);
        test.setKeyMarshaller(LongMarshaller.INSTANCE);
        test.setValueMarshaller(HashSetStringMarshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        tx =  pf.tx();
        String val = new String(new byte[1024]);
        final long numMessages = 10;
        final int numConsumers = 200;

        for (long i=0; i<numMessages; i++) {
            HashSet<String> hs = new HashSet<String>();
            for (int j=0; j<numConsumers;j++) {
                hs.add(val + "SOME TEXT" + j);
            }
            test.put(tx, i, hs);
        }
        tx.commit();
        tx =  pf.tx();
        for (long i=0; i<numMessages; i++) {
            HashSet<String> hs = new HashSet<String>();
            for (int j=numConsumers; j<numConsumers*2;j++) {
                hs.add(val + "SOME TEXT" + j);
            }
            test.put(tx, i, hs);
        }

        tx.commit();
        tx =  pf.tx();
        for (long i=0; i<numMessages; i++) {
            assertTrue(test.containsKey(tx, i));
            test.get(tx, i);
        }
        tx.commit();
    }

    public void testLargeValueOverflow() throws Exception {
        pf = new PageFile(directory, getClass().getName());
        pf.setPageSize(4*1024);
        pf.setWriteBatchSize(1);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        BTreeIndex<Long, String> test = new BTreeIndex<Long, String>(pf, id);
        test.setKeyMarshaller(LongMarshaller.INSTANCE);
        test.setValueMarshaller(StringMarshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        final int stringSize = 6*1024;
        tx =  pf.tx();
        String val = new String(new byte[stringSize]);
        final long numMessages = 1;

        for (long i=0; i<numMessages; i++) {
            test.put(tx, i, val);
        }
        tx.commit();

        exerciseAnotherIndex(tx);

        tx =  pf.tx();
        for (long i=0; i<numMessages; i++) {
            assertTrue(test.containsKey(tx, i));
            String s = test.get(tx, i);
            assertEquals("len is as expected", stringSize, s.length());
        }
        tx.commit();
    }

    public void exerciseAnotherIndex(Transaction tx) throws Exception {
        long id = tx.allocate().getPageId();

        ListIndex<String, String> test = new ListIndex<String, String>(pf, id);
        test.setKeyMarshaller(StringMarshaller.INSTANCE);
        test.setValueMarshaller(StringMarshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        final int count = 10000;

        String payload = new String(new byte[1]);
        for (int i = 0; i < count; i++) {
            test.put(tx, key(i), String.valueOf(i) + payload);
        }
        tx.commit();

        test.clear(tx);
        tx.commit();
    }

    public void testListIndexConsistancyOverTime() throws Exception {

        final int NUM_ITERATIONS = 50;

        pf = new PageFile(directory, getClass().getName());
        pf.setPageSize(4*1024);
        //pf.setEnablePageCaching(false);
        pf.setWriteBatchSize(1);
        pf.load();
        tx = pf.tx();
        long id = tx.allocate().getPageId();

        ListIndex<String, String> test = new ListIndex<String, String>(pf, id);
        test.setKeyMarshaller(StringMarshaller.INSTANCE);
        test.setValueMarshaller(StringMarshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        int expectedListEntries = 0;
        int nextSequenceId = 0;

        LOG.info("Loading up the ListIndex with "+NUM_ITERATIONS+" entires and sparsely populating the sequences.");

        for (int i = 0; i < NUM_ITERATIONS; ++i) {
            test.add(tx, String.valueOf(expectedListEntries++), new String("AA"));

            for (int j = 0; j < expectedListEntries; j++) {

                String sequenceSet = test.get(tx, String.valueOf(j));

                int startSequenceId = nextSequenceId;
                for (int ix = 0; ix < NUM_ITERATIONS; ix++) {
                    sequenceSet.concat(String.valueOf(nextSequenceId++));
                    test.put(tx, String.valueOf(j), sequenceSet);
                }

                sequenceSet = test.get(tx, String.valueOf(j));

                for (int ix = 0; ix < NUM_ITERATIONS - 1; ix++) {
                    //sequenceSet.remove(startSequenceId++);
                    test.put(tx, String.valueOf(j), String.valueOf(j));
                }
            }
        }

        exerciseAnotherIndex(tx);
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

    void doInsertReverse(int count) throws Exception {
        for (int i = count-1; i >= 0; i--) {
            index.put(tx, key(i), (long)i);
            tx.commit();
        }
    }
    /**
     * Overriding so that this generates keys that are the worst case for the BTree. Keys that
     * always insert to the end of the BTree.  
     */
    @Override
    protected String key(int i) {
        return "key:"+nf.format(i);
    }

    static class HashSetStringMarshaller extends VariableMarshaller<HashSet<String>> {
        final static HashSetStringMarshaller INSTANCE = new HashSetStringMarshaller();

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
