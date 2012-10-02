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

import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.StringMarshaller;

public class HashIndexBenchMark extends IndexBenchmark {

    @Override
    protected Index<String, Long> createIndex() throws Exception {

        Transaction tx = pf.tx();
        long id = tx.allocate().getPageId();
        tx.commit();

        HashIndex<String, Long> index = new HashIndex<String, Long>(pf, id);
        index.setKeyMarshaller(StringMarshaller.INSTANCE);
        index.setValueMarshaller(LongMarshaller.INSTANCE);
        
        return index;
    }

}
