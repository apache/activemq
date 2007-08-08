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
package org.apache.activemq.broker;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.activemq.store.PersistenceAdapter;

public class BrokerRestartTestSupport extends BrokerTestSupport {

    private PersistenceAdapter persistenceAdapter;

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        persistenceAdapter = broker.getPersistenceAdapter();
        return broker;
    }

    /**
     * @return
     * @throws Exception
     */
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistenceAdapter(persistenceAdapter);
        return broker;
    }

    /**
     * Simulates a broker restart. The memory based persistence adapter is
     * reused so that it does not "loose" it's "persistent" messages.
     * 
     * @throws IOException
     * @throws URISyntaxException
     */
    protected void restartBroker() throws Exception {
        broker.stop();
        broker = createRestartedBroker();
        broker.start();
    }

}
