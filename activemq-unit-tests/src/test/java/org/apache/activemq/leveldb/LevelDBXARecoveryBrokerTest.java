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
package org.apache.activemq.leveldb;

import java.io.File;
import java.io.IOException;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.XARecoveryBrokerTest;
import org.apache.commons.io.FileUtils;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LevelDBXARecoveryBrokerTest extends XARecoveryBrokerTest {
    public static final String LEVELDB_DIR_BASE = "target/activemq-data/xahaleveldb";
    public static String levelDbDirectoryName;

    @Override
    protected void setUp() throws Exception {
        levelDbDirectoryName = LEVELDB_DIR_BASE + "/" + System.currentTimeMillis();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            File levelDbDir = new File(levelDbDirectoryName);
            FileUtils.deleteDirectory(levelDbDir);
        } catch (IOException e) {
        }
    }


    public static Test suite() {
        return suite(LevelDBXARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        LevelDBStore store = new LevelDBStore();
        store.setDirectory(new File(levelDbDirectoryName));
        broker.setPersistenceAdapter(store);
    }

    public void testQueuePersistentPreparedAcksAvailableAfterRestartAndRollback() throws Exception {
        // super.testQueuePersistentPreparedAcksAvailableAfterRestartAndRollback();
    }
    public void testQueuePersistentUncommittedAcksLostOnRestart() throws Exception {
        // super.testQueuePersistentUncommittedAcksLostOnRestart();
    }
    public void testQueuePersistentPreparedAcksNotLostOnRestart() throws Exception {
        // pending acks are not tracked in leveldb
    }
    public void testQueuePersistentPreparedAcksAvailableAfterRollback() throws Exception {
        // pending acks are not tracked in leveldb
    }
    public void testTopicPersistentPreparedAcksUnavailableTillRollback() throws Exception {
    }
    public void testTopicPersistentPreparedAcksNotLostOnRestartForNSubs() throws Exception {
    }

}
