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
package org.apache.activemq;

import jakarta.jms.TextMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class VmTransportBrokerRestartTest extends EmbeddedBrokerTestSupport {

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();

        broker.setBrokerName("localhost");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");

        return broker;
    }

    @Test
    public void testSendReceiveAfterBrokerRestart() throws Exception {

        String firstMessage = "message-before-restart";
        template.convertAndSend(firstMessage);

        TextMessage receivedBefore =
                (TextMessage) template.receive(destination);

        assertNotNull(receivedBefore);
        assertEquals(firstMessage, receivedBefore.getText());

        broker.stop();
        broker.waitUntilStopped();

        broker = createBroker();
        startBroker();
        broker.waitUntilStarted();

        connectionFactory = createConnectionFactory();
        template = createJmsTemplate();
        template.setDefaultDestination(destination);
        template.afterPropertiesSet();

        String secondMessage = "message-after-restart";
        template.convertAndSend(secondMessage);

        TextMessage receivedAfter =
                (TextMessage) template.receive(destination);

        assertNotNull(receivedAfter);
        assertEquals(secondMessage, receivedAfter.getText());
    }

    @After
    public void cleanup() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }
}