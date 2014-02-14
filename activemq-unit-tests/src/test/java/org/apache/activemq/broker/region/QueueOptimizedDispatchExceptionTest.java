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
package org.apache.activemq.broker.region;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.MemoryUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueOptimizedDispatchExceptionTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueueOptimizedDispatchExceptionTest.class);

    private static final String brokerName = "testBroker";
    private static final String brokerUrl = "vm://" + brokerName;
    private static final int count = 50;

    private final static String mesageIdRoot = "11111:22222:";
    private final ActiveMQQueue destination = new ActiveMQQueue("queue-"
            + QueueOptimizedDispatchExceptionTest.class.getSimpleName());
    private final int messageBytesSize = 256;
    private final String text = new String(new byte[messageBytesSize]);

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {

        // Setup and start the broker
        broker = new BrokerService();
        broker.setBrokerName(brokerName);
        broker.setPersistent(false);
        broker.setSchedulerSupport(false);
        broker.setUseJmx(false);
        broker.setUseShutdownHook(false);
        broker.addConnector(brokerUrl);

        // Start the broker
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    private class MockMemoryUsage extends MemoryUsage {

        private boolean full = true;

        public void setFull(boolean full) {
            this.full = full;
        }

        @Override
        public boolean isFull() {
            return full;
        }
    }

    @Test
    public void TestOptimizedDispatchCME() throws Exception {
        final PersistenceAdapter persistenceAdapter = broker.getPersistenceAdapter();
        final MessageStore queueMessageStore =
            persistenceAdapter.createQueueMessageStore(destination);
        final ConnectionContext contextNotInTx = new ConnectionContext();
        contextNotInTx.setConnection(new Connection() {

            @Override
            public void stop() throws Exception {
            }

            @Override
            public void start() throws Exception {
            }

            @Override
            public void updateClient(ConnectionControl control) {
            }

            @Override
            public void serviceExceptionAsync(IOException e) {
            }

            @Override
            public void serviceException(Throwable error) {
            }

            @Override
            public Response service(Command command) {
                return null;
            }

            @Override
            public boolean isSlow() {
                return false;
            }

            @Override
            public boolean isNetworkConnection() {
                return false;
            }

            @Override
            public boolean isManageable() {
                return false;
            }

            @Override
            public boolean isFaultTolerantConnection() {
                return false;
            }

            @Override
            public boolean isConnected() {
                return true;
            }

            @Override
            public boolean isBlocked() {
                return false;
            }

            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public ConnectionStatistics getStatistics() {
                return null;
            }

            @Override
            public String getRemoteAddress() {
                return null;
            }

            @Override
            public int getDispatchQueueSize() {
                return 0;
            }

            @Override
            public Connector getConnector() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public String getConnectionId() {
                return null;
            }

            @Override
            public void dispatchSync(Command message) {
            }

            @Override
            public void dispatchAsync(Command command) {
            }

            @Override
            public int getActiveTransactionCount() {
                return 0;
            }

            @Override
            public Long getOldestActiveTransactionDuration() {
                return null;
            }
        });

        final DestinationStatistics destinationStatistics = new DestinationStatistics();
        final Queue queue = new Queue(broker, destination,
                queueMessageStore, destinationStatistics, broker.getTaskRunnerFactory());

        final MockMemoryUsage usage = new MockMemoryUsage();

        queue.setOptimizedDispatch(true);
        queue.initialize();
        queue.start();
        queue.memoryUsage = usage;

        ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        ProducerInfo producerInfo = new ProducerInfo();
        ProducerState producerState = new ProducerState(producerInfo);
        producerExchange.setProducerState(producerState);
        producerExchange.setConnectionContext(contextNotInTx);

        // populate the queue store, exceed memory limit so that cache is disabled
        for (int i = 0; i < count; i++) {
            Message message = getMessage(i);
            queue.send(producerExchange, message);
        }

        usage.setFull(false);

        try {
            queue.wakeup();
        } catch(Exception e) {
            LOG.error("Queue threw an unexpected exception: " + e.toString());
            fail("Should not throw an exception.");
        }
    }

    private Message getMessage(int i) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setMessageId(new MessageId(mesageIdRoot + i));
        message.setDestination(destination);
        message.setPersistent(false);
        message.setResponseRequired(true);
        message.setText("Msg:" + i + " " + text);
        assertEquals(message.getMessageId().getProducerSequenceId(), i);
        return message;
    }
}
