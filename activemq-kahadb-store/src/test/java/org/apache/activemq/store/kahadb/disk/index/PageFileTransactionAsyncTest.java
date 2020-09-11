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

import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LongMarshaller;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.junit.Test;

import java.io.File;
import java.util.LinkedList;

public class PageFileTransactionAsyncTest {

    @Test(timeout=60000)
    public void testLargeTransactionPageOkForReuse() throws Exception {

        int numPagesToExercise = 4000;

        PageFile pf = new PageFile(new File("target/test-data"), "PageFileTransactionAsyncTest");
        pf.setEnableWriteThread(true);
        pf.setWriteBatchSize(5000);
        pf.load();

        LinkedList<Index> indices = new LinkedList<>();
        for (int i=0; i<numPagesToExercise; i++) {
            indices.add(createIndex(pf));
        }

        // large tx
        Transaction tx = pf.tx();
        for (Index i : indices) {
            for (int j=0; j<10; j++) {
                i.put(tx, key(j), (long) j);
            }
        }
        tx.commit();

        // with some of those writes disk bound, exercise again
        for (Index i : indices) {
            tx = pf.tx();
            i.put(tx, key(0), (long)0);
            tx.commit();
        }

        // lets see if it is ok!
        pf.flush();
        pf.unload();
        pf.delete();
    }

    private String key(int v) {
        return "key:"+v;
    }

    protected Index<String, Long> createIndex(PageFile pf) throws Exception {

        Transaction tx = pf.tx();
        long id = tx.allocate().getPageId();

        BTreeIndex<String, Long> index = new BTreeIndex<String,Long>(pf, id);
        index.setKeyMarshaller(StringMarshaller.INSTANCE);
        index.setValueMarshaller(LongMarshaller.INSTANCE);
        index.load(tx);
        tx.commit();

        return index;
    }

}
