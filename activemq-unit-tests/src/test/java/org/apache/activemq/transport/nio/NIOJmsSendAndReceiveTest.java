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
package org.apache.activemq.transport.nio;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.test.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.junit.experimental.categories.Category;

/**
 * 
 */
@Category(ParallelTest.class)
public class NIOJmsSendAndReceiveTest extends JmsTopicSendReceiveWithTwoConnectionsTest {
    protected BrokerService broker;

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
            broker.start();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() {
        // Use the actual bound URI instead of the bind URI with port 0
        String connectUrl = getBrokerURL();
        try {
            if (broker != null && !broker.getTransportConnectors().isEmpty()) {
                connectUrl = broker.getTransportConnectors().get(0).getPublishableConnectString();
            }
        } catch (Exception e) {
            // Fall back to bind URL if we can't get the actual URL
        }
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectUrl);
        return connectionFactory;
    }

    protected String getBrokerURL() {
        return "nio://localhost:0";
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.addConnector(getBrokerURL());
        return answer;
    }
}
