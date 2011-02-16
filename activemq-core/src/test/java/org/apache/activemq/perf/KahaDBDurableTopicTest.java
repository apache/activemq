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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * 
 */
public class KahaDBDurableTopicTest extends SimpleDurableTopicTest {

    @Override
    protected void setUp() throws Exception {
        //this.initialConsumerDelay = 10 * 1000;
        super.setUp();
    }
    
    @Override
    protected ActiveMQConnectionFactory createConnectionFactory(String uri) throws Exception {
        ActiveMQConnectionFactory result = new ActiveMQConnectionFactory(uri);
        //result.setDispatchAsync(false);
        return result;
    }
    
    @Override
    protected void configureBroker(BrokerService answer,String uri) throws Exception {
        //AMQPersistenceAdapterFactory persistenceFactory = new AMQPersistenceAdapterFactory();
        //persistenceFactory.setMaxFileLength(1024*16);
        //persistenceFactory.setPersistentIndex(true);
        //persistenceFactory.setCleanupInterval(10000);
        //answer.setPersistenceFactory(persistenceFactory);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(uri);
        answer.setUseShutdownHook(false);
        answer.setEnableStatistics(false);
    }
}
