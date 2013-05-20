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
package org.apache.activemq.store.leveldb;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.cursors.NegativeQueueTest;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.util.IOHelper;

import java.io.File;

public class LevelDBNegativeQueueTest extends NegativeQueueTest {

    @Override
    protected void configureBroker(BrokerService answer) throws Exception {
        super.configureBroker(answer);
        LevelDBStore levelDBStore = new LevelDBStore();
        File directory = new File("target/activemq-data/leveldb");
        IOHelper.deleteChildren(directory);
        levelDBStore.setDirectory(directory);
        levelDBStore.deleteAllMessages();
        answer.setPersistenceAdapter(levelDBStore);
    }
}
