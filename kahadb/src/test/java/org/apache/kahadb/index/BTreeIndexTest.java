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

import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.index.Index;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.StringMarshaller;

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
}
