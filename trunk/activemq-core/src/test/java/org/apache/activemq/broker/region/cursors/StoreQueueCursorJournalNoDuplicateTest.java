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

package org.apache.activemq.broker.region.cursors;

import java.io.File;

import org.apache.activeio.journal.active.JournalImpl;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.journal.JournalPersistenceAdapter;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;

/**
 * @author gtully
 * @see https://issues.apache.org/activemq/browse/AMQ-2020
 **/
public class StoreQueueCursorJournalNoDuplicateTest extends StoreQueueCursorNoDuplicateTest {
    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        
        File dataFileDir = new File("target/activemq-data/StoreQueueCursorJournalNoDuplicateTest");
        File journalDir = new File(dataFileDir, "journal").getCanonicalFile();
        JournalImpl journal = new JournalImpl(journalDir, 3, 1024 * 1024 * 20);

        KahaPersistenceAdapter kahaAdaptor = new KahaPersistenceAdapter();
        kahaAdaptor.setDirectory(dataFileDir);
        JournalPersistenceAdapter journalAdaptor = new JournalPersistenceAdapter(journal, kahaAdaptor, broker.getTaskRunnerFactory());
        journalAdaptor.setMaxCheckpointWorkers(1);

        broker.setPersistenceAdapter(journalAdaptor);
        return broker;
    }
}
