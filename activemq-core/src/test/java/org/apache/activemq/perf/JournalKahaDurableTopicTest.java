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
import org.apache.activeio.journal.active.JournalImpl;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.journal.JournalPersistenceAdapter;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;

/**
 * @version $Revision: 1.3 $
 */
public class JournalKahaDurableTopicTest extends SimpleDurableTopicTest {

    protected void configureBroker(BrokerService answer) throws Exception {

        File dataFileDir = new File("target/test-amq-data/perfTest");
        File journalDir = new File(dataFileDir, "journal").getCanonicalFile();
        JournalImpl journal = new JournalImpl(journalDir, 3, 1024 * 1024 * 20);

        KahaPersistenceAdapter kahaAdaptor = new KahaPersistenceAdapter();
        JournalPersistenceAdapter journalAdaptor = new JournalPersistenceAdapter(journal, kahaAdaptor, answer.getTaskRunnerFactory());
        journalAdaptor.setMaxCheckpointWorkers(1);

        answer.setPersistenceAdapter(journalAdaptor);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);

    }

}
