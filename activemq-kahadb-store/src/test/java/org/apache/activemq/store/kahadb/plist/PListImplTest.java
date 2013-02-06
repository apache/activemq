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

import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.PListTestSupport;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PListImplTest extends PListTestSupport {


    @Override
    protected PListStoreImpl createPListStore() {
        return new PListStoreImpl();
    }

    protected PListStore createConcurrentAddIteratePListStore() {
        PListStoreImpl store = createPListStore();
        store.setIndexPageSize(2 * 1024);
        store.setJournalMaxFileLength(1024 * 1024);
        store.setCleanupInterval(-1);
        store.setIndexEnablePageCaching(false);
        store.setIndexWriteBatchSize(100);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddRemovePListStore() {
        PListStoreImpl store = createPListStore();
        store.setCleanupInterval(400);
        store.setJournalMaxFileLength(1024*5);
        store.setLazyInit(false);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddRemoveWithPreloadPListStore() {
        PListStoreImpl store = createPListStore();
        store.setJournalMaxFileLength(1024*5);
        store.setCleanupInterval(5000);
        store.setIndexWriteBatchSize(500);
        return store;
    }

    @Override
    protected PListStore createConcurrentAddIterateRemovePListStore(boolean enablePageCache) {
        PListStoreImpl store = createPListStore();
        store.setIndexEnablePageCaching(enablePageCache);
        store.setIndexPageSize(2*1024);
        return store;
    }
}
