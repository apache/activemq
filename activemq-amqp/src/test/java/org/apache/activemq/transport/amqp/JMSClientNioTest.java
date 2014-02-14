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
package org.apache.activemq.transport.amqp;

import javax.jms.JMSException;

import org.junit.Test;

/**
 * Test the JMS client when connected to the NIO transport.
 */
public class JMSClientNioTest extends JMSClientTest {

    @Override
    @Test
    public void testProducerConsume() throws Exception {
    }

    @Override
    @Test
    public void testTransactedConsumer() throws Exception {
    }

    @Override
    @Test
    public void testRollbackRececeivedMessage() throws Exception {
    }

    @Override
    @Test
    public void testTXConsumerAndLargeNumberOfMessages() throws Exception {
    }

    @Override
    @Test
    public void testSelectors() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testProducerThrowsWhenBrokerStops() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testProducerCreateThrowsWhenBrokerStops() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testConsumerCreateThrowsWhenBrokerStops() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testConsumerReceiveNoWaitThrowsWhenBrokerStops() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testConsumerReceiveTimedThrowsWhenBrokerStops() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testConsumerReceiveReturnsBrokerStops() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testBrokerRestartWontHangConnectionClose() throws Exception {
    }

    @Override
    @Test(timeout=120000)
    public void testProduceAndConsumeLargeNumbersOfMessages() throws JMSException {
    }

    @Override
    @Test(timeout=30000)
    public void testSyncSends() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testDurableConsumerAsync() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testDurableConsumerSync() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testTopicConsumerAsync() throws Exception {
    }

    @Override
    @Test(timeout=45000)
    public void testTopicConsumerSync() throws Exception {
    }

    @Override
    @Test(timeout=60000)
    public void testConnectionsAreClosed() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testExecptionListenerCalledOnBrokerStop() throws Exception {
    }

    @Override
    @Test(timeout=30000)
    public void testSessionTransactedCommit() throws JMSException, InterruptedException {
    }

    @Override
    protected int getBrokerPort() {
        return nioPort;
    }
}
