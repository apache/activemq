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
package org.apache.activemq.perf;

import java.io.File;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.IOHelper;

public class RunBroker {

    public static void main(String arg[]) {

        try {
            KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
            File dataFileDir = new File("target/test-amq-data/perfTest/kahadb");
            IOHelper.deleteChildren(dataFileDir);
            kahaDB.setDirectory(dataFileDir);

            // The setEnableJournalDiskSyncs(false) setting is a little
            // dangerous right now, as I have not verified
            // what happens if the index is updated but a journal update is
            // lost.
            // Index is going to be in consistent, but can it be repaired?
            // kaha.setEnableJournalDiskSyncs(false);
            // Using a bigger journal file size makes he take fewer spikes as it
            // is not switching files as often.
            // kaha.setJournalMaxFileLength(1024*1024*100);

            // small batch means more frequent and smaller writes
            kahaDB.setIndexWriteBatchSize(1000);
            kahaDB.setIndexCacheSize(10000);

            // do the index write in a separate thread
            // kahaDB.setEnableIndexWriteAsync(true);
            BrokerService broker = new BrokerService();
            broker.setUseJmx(false);
            // broker.setPersistenceAdapter(adaptor);
            broker.setPersistenceAdapter(kahaDB);
            // broker.setPersistent(false);
            broker.setDeleteAllMessagesOnStartup(true);
            broker.addConnector("tcp://0.0.0.0:61616");
            broker.start();
            System.err.println("Running");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}
