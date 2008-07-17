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
package org.apache.activemq.kaha.impl.index.hash;

import java.io.File;

import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.impl.index.Index;
import org.apache.activemq.kaha.impl.index.IndexBenchmark;
import org.apache.activemq.kaha.impl.index.hash.HashIndex;

public class HashIndexBenchMark extends IndexBenchmark {

    @Override
    protected Index createIndex(File root, String name) throws Exception {
        HashIndex index = new HashIndex(root, name, indexManager);
        index.setNumberOfBins(12);
        index.setPageSize(32 * 1024);
        index.setKeyMarshaller(Store.STRING_MARSHALLER);
        return index;
    }

}
