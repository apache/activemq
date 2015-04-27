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
package org.apache.activemq.store.kahadb;

import java.io.File;

import org.apache.activemq.store.MessageStore;

/**
 * This test is for AMQ-5748 to verify that {@link MessageStore} implements correctly
 * compute the size of the messages in the KahaDB Store.
 *
 */
public class KahaDBMessageStoreSizeTest extends AbstractKahaDBMessageStoreSizeTest {

    @Override
    protected void createStore(boolean deleteAllMessages, String directory) throws Exception {
        KahaDBStore kahaDBStore = new KahaDBStore();
        store = kahaDBStore;
        kahaDBStore.setJournalMaxFileLength(1024 * 512);
        kahaDBStore.setDeleteAllMessages(deleteAllMessages);
        kahaDBStore.setDirectory(new File(directory));
        kahaDBStore.start();
        messageStore = store.createQueueMessageStore(destination);
        messageStore.start();
    }

    @Override
    protected String getVersion5Dir() {
        return "src/test/resources/org/apache/activemq/store/kahadb/MessageStoreTest/version5";
    }
}
