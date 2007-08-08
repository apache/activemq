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
package org.apache.activemq.usecases;

import java.io.File;
import java.io.IOException;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.journal.JournalPersistenceAdapterFactory;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class JDBCDurableSubscriptionTest extends DurableSubscriptionTestSupport {

    protected PersistenceAdapter createPersistenceAdapter() throws IOException {
        File dataDir = new File("target/test-data/durableJDBC");
        JournalPersistenceAdapterFactory factory = new JournalPersistenceAdapterFactory();
        factory.setDataDirectoryFile(dataDir);
        factory.setUseJournal(false);
        return factory.createPersistenceAdapter();
    }

}
