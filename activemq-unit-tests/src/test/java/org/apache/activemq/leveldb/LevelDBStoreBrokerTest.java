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

import junit.framework.Test;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.activemq.store.PersistenceAdapter;

import java.io.File;
import java.io.IOException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LevelDBStoreBrokerTest extends BrokerTest {

    public static Test suite() {
        return suite(LevelDBStoreBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistenceAdapter(createPersistenceAdapter(true));
        return broker;
    }

    protected PersistenceAdapter createPersistenceAdapter(boolean delete) {
        LevelDBStore store  = new LevelDBStore();
        store.setDirectory(new File("target/activemq-data/leveldb"));
        if (delete) {
          store.deleteAllMessages();
        }
        return store;
      }

      protected BrokerService createRestartedBroker() throws IOException {
        BrokerService broker = new BrokerService();
        broker.setPersistenceAdapter(createPersistenceAdapter(false));
        return broker;
      }

}
