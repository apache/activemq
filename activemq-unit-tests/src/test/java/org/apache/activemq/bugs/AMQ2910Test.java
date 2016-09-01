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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleClientsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@RunWith(BlockJUnit4ClassRunner.class)
public class AMQ2910Test extends JmsMultipleClientsTestSupport {

    final int maxConcurrency = 60;
    final int msgCount = 200;
    final Vector<Throwable> exceptions = new Vector<Throwable>();

    @Override
    protected BrokerService createBroker() throws Exception {
        //persistent = true;
        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://localhost:0");
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setPendingQueuePolicy(new FilePendingQueueMessageStoragePolicy());
        defaultEntry.setCursorMemoryHighWaterMark(50);
        defaultEntry.setMemoryLimit(500*1024);
        defaultEntry.setProducerFlowControl(false);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        broker.getSystemUsage().getMemoryUsage().setLimit(1000 * 1024);

        return broker;
    }

    @Test(timeout = 120 * 1000)
    public void testConcurrentSendToPendingCursor() throws Exception {
        final ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
        factory.setCloseTimeout(30000);
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i=0; i<maxConcurrency; i++) {
            final ActiveMQQueue dest = new ActiveMQQueue("Queue-" + i);
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendMessages(factory.createConnection(), dest, msgCount);
                    } catch (Throwable t) {
                        exceptions.add(t);
                    }
                }
            });
        }

        executor.shutdown();

        assertTrue("send completed", executor.awaitTermination(60, TimeUnit.SECONDS));
        assertNoExceptions();

        executor = Executors.newCachedThreadPool();
        for (int i=0; i<maxConcurrency; i++) {
            final ActiveMQQueue dest = new ActiveMQQueue("Queue-" + i);
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        startConsumers(factory, dest);
                    } catch (Throwable t) {
                        exceptions.add(t);
                    }
                }
            });
        }

        executor.shutdown();
        assertTrue("consumers completed", executor.awaitTermination(30, TimeUnit.SECONDS));

        allMessagesList.setMaximumDuration(90*1000);
        final int numExpected = maxConcurrency * msgCount;
        allMessagesList.waitForMessagesToArrive(numExpected);

        if (allMessagesList.getMessageCount() != numExpected) {
            dumpAllThreads(getName());

        }
        allMessagesList.assertMessagesReceivedNoWait(numExpected);

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());

    }

    private void assertNoExceptions() {
        if (!exceptions.isEmpty()) {
            for (Throwable t: exceptions) {
                t.printStackTrace();
            }
        }
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }
}
