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
package org.apache.activemq.store;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.apache.commons.io.FileUtils;

public class MKahaDbSharedLockerTest extends TestCase {


    public void testBrokerShutdown() throws Exception {
        final BrokerService master = new BrokerService();
        master.setBrokerName("master");

        master.setUseJmx(false);
        master.setPersistent(true);
        master.setDeleteAllMessagesOnStartup(true);

        MultiKahaDBPersistenceAdapter mKahaDB = new MultiKahaDBPersistenceAdapter();
        mKahaDB.setDirectory(new File("target/test/kahadb"));
        List adapters = new LinkedList<FilteredKahaDBPersistenceAdapter>();
        FilteredKahaDBPersistenceAdapter defaultEntry = new FilteredKahaDBPersistenceAdapter();
        defaultEntry.setPersistenceAdapter(new KahaDBPersistenceAdapter());
        defaultEntry.setPerDestination(true);
        adapters.add(defaultEntry);

        mKahaDB.setFilteredPersistenceAdapters(adapters);
        master.setPersistenceAdapter(mKahaDB);

        SharedFileLocker sharedFileLocker = new SharedFileLocker();
        mKahaDB.setLockKeepAlivePeriod(1000);
        mKahaDB.setLocker(sharedFileLocker);

        master.start();
        master.waitUntilStarted();

        FileUtils.forceDelete(new File("target/test/kahadb/lock"));

        assertTrue("broker should be stopped now", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return master.isStopped();
            }
        }));

        master.stop();
        master.waitUntilStopped();
    }

}