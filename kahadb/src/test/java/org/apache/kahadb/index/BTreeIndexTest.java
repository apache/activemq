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

import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.StringMarshaller;
import org.apache.kahadb.util.VariableMarshaller;

public class BTreeIndexTest extends IndexTestSupport {

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
        createPageFileAndIndex(100);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);
        tx.commit();
          
        // Insert in reverse order..
        doInsertReverse(1000);
        
        this.index.unload(tx);
        tx.commit();
        this.index.load(tx);
        tx.commit();

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
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
        this.index.load(tx);
        tx.commit();

        final int count = 4000;
        doInsert(count);

        index.remove(tx, key(3697));
        index.remove(tx, key(1566));
    }

    public void x_testLargeValue() throws Exception {
        createPageFileAndIndex(4*1024);
        long id = tx.allocate().getPageId();
        tx.commit();

        BTreeIndex<Long, HashSet<String>> test = new BTreeIndex<Long, HashSet<String>>(pf, id);
        test.setKeyMarshaller(LongMarshaller.INSTANCE);
        test.setValueMarshaller(HashSetStringMarshaller.INSTANCE);
        test.load(tx);
        tx.commit();

        tx =  pf.tx();
        String val = new String(new byte[93]);
        final long numMessages = 2000;
        final int numConsumers = 10000;

        for (long i=0; i<numMessages; i++) {
            HashSet<String> hs = new HashSet<String>();
            for (int j=0; j<numConsumers;j++) {
                hs.add(val + "SOME TEXT" + j);
            }
            test.put(tx, i, hs);
        }

        for (long i=0; i<numMessages; i++) {
            test.get(tx, i);
        }
        tx.commit();
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
