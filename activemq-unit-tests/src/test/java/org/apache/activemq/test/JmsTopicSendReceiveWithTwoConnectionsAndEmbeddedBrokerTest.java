/*
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
package org.apache.activemq.test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

public class JmsTopicSendReceiveWithTwoConnectionsAndEmbeddedBrokerTest extends JmsTopicSendReceiveWithTwoConnectionsTest {

    protected BrokerService broker;
    protected String bindAddress = "tcp://localhost:0";
    protected String connectionAddress;

    /**
     * Sets up a test where the producer and consumer have their own connection.
     *
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    /**
     * Factory method to create a new broker
     *
     * @throws Exception
     */
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        TransportConnector connector = configureBroker(answer);
        answer.start();

        connectionAddress = connector.getPublishableConnectString();

        return answer;
    }

    protected TransportConnector configureBroker(BrokerService answer) throws Exception {
        return answer.addConnector(bindAddress);
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionAddress);
    }
}
