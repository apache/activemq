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

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LongMarshaller;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;

public class BTreeIndexBenchMark extends IndexBenchmark {

    private NumberFormat nf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        nf = NumberFormat.getIntegerInstance();
        nf.setMinimumIntegerDigits(10);
        nf.setGroupingUsed(false);
    }
    
    @Override
    protected Index<String, Long> createIndex() throws Exception {

        Transaction tx = pf.tx();
        long id = tx.allocate().getPageId();
        tx.commit();

        BTreeIndex<String, Long> index = new BTreeIndex<String, Long>(pf, id);
        index.setKeyMarshaller(StringMarshaller.INSTANCE);
        index.setValueMarshaller(LongMarshaller.INSTANCE);
        
        return index;
    }
    
    @Override
    protected void dumpIndex(Index<String, Long> index) throws IOException {
        Transaction tx = pf.tx();
        ((BTreeIndex)index).printStructure(tx, System.out);
    }

    /**
     * Overriding so that this generates keys that are the worst case for the BTree. Keys that
     * always insert to the end of the BTree.  
     */
    @Override
    protected String key(long i) {
        return "a-long-message-id-like-key:"+nf.format(i);
    }

}
