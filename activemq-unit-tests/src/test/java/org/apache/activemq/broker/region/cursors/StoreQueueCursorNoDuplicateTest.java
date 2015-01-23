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

package org.apache.activemq.broker.region.cursors;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gtully
 * https://issues.apache.org/activemq/browse/AMQ-2020
 **/
public class StoreQueueCursorNoDuplicateTest extends TestCase {
    static final Logger LOG = LoggerFactory.getLogger(StoreQueueCursorNoDuplicateTest.class);
            ActiveMQQueue destination = new ActiveMQQueue("queue-"
            + StoreQueueCursorNoDuplicateTest.class.getSimpleName());
    BrokerService brokerService;

    final static String mesageIdRoot = "11111:22222:0:";
    final int messageBytesSize = 1024;
    final String text = new String(new byte[messageBytesSize]);

    protected int count = 6;

    @Override
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.setUseJmx(false);
        brokerService.deleteAllMessages();
        brokerService.start();
    }

    protected BrokerService createBroker() throws Exception {
        return new BrokerService();
    }

    @Override
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    public void testNoDuplicateAfterCacheFullAndReadPast() throws Exception {
        final PersistenceAdapter persistenceAdapter = brokerService
                .getPersistenceAdapter();
        final MessageStore queueMessageStore = persistenceAdapter
                .createQueueMessageStore(destination);
        final ConsumerInfo consumerInfo = new ConsumerInfo();
        final DestinationStatistics destinationStatistics = new DestinationStatistics();
        consumerInfo.setExclusive(true);

        final Queue queue = new Queue(brokerService, destination,
                queueMessageStore, destinationStatistics, null);

        queueMessageStore.start();
        queueMessageStore.registerIndexListener(null);

        QueueStorePrefetch underTest = new QueueStorePrefetch(queue, brokerService.getBroker());
        SystemUsage systemUsage = new SystemUsage();
        // ensure memory limit is reached
        systemUsage.getMemoryUsage().setLimit(messageBytesSize * (count + 2));
        underTest.setSystemUsage(systemUsage);
        underTest.setEnableAudit(false);
        underTest.start();
        assertTrue("cache enabled", underTest.isUseCache() && underTest.isCacheEnabled());

        final ConnectionContext contextNotInTx = new ConnectionContext();
        for (int i = 0; i < count; i++) {
            ActiveMQTextMessage msg = getMessage(i);
            msg.setMemoryUsage(systemUsage.getMemoryUsage());

            queueMessageStore.addMessage(contextNotInTx, msg);
            underTest.addMessageLast(msg);
        }

        assertTrue("cache is disabled as limit reached", !underTest.isCacheEnabled());
        int dequeueCount = 0;

        underTest.setMaxBatchSize(2);
        underTest.reset();
        while (underTest.hasNext() && dequeueCount < count) {
            MessageReference ref = underTest.next();
            ref.decrementReferenceCount();
            underTest.remove();
            LOG.info("Received message: {} with body: {}",
                     ref.getMessageId(), ((ActiveMQTextMessage)ref.getMessage()).getText());
            assertEquals(dequeueCount++, ref.getMessageId().getProducerSequenceId());
        }
        underTest.release();
        assertEquals(count, dequeueCount);
    }

    private ActiveMQTextMessage getMessage(int i) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        MessageId id = new MessageId(mesageIdRoot + i);
        id.setBrokerSequenceId(i);
        id.setProducerSequenceId(i);
        message.setMessageId(id);
        message.setDestination(destination);
        message.setPersistent(true);
        message.setResponseRequired(true);
        message.setText("Msg:" + i + " " + text);
        assertEquals(message.getMessageId().getProducerSequenceId(), i);
        return message;
    }
}
