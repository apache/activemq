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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.memory.MemoryMessageStore;
import org.apache.activemq.usage.SystemUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemoryMessageStoreQueueCursorTest {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessageStoreQueueCursorTest.class);

    ActiveMQQueue destination = new ActiveMQQueue("queue-" + MemoryMessageStoreQueueCursorTest.class.getSimpleName());
    BrokerService brokerService;

    final static String mesageIdRoot = "11111:22222:0:";
    final int messageBytesSize = 1024;
    final String text = new String(new byte[messageBytesSize]);

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
        brokerService.start();
    }

    protected BrokerService createBroker() throws Exception {
        return new BrokerService();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    @Test(timeout = 10000)
    public void testRecoverNextMessages2() throws Exception {
        final MemoryMessageStore queueMessageStore = new MemoryMessageStore(destination);

        final DestinationStatistics destinationStatistics = new DestinationStatistics();
        final Queue queue = new Queue(brokerService, destination, queueMessageStore, destinationStatistics, null);

        queueMessageStore.start();
        queueMessageStore.registerIndexListener(null);

        QueueStorePrefetch myCursor = new QueueStorePrefetch(queue, brokerService.getBroker());
        SystemUsage systemUsage = new SystemUsage();
        // ensure memory limit is reached
        systemUsage.getMemoryUsage().setLimit(messageBytesSize * 5);
        myCursor.setSystemUsage(systemUsage);
        myCursor.setEnableAudit(false);
        myCursor.start();
        assertTrue("cache enabled", myCursor.isUseCache() && myCursor.isCacheEnabled());


        ActiveMQTextMessage msg0 = getMessage(0);
        msg0.setMemoryUsage(systemUsage.getMemoryUsage());
        queueMessageStore.addMessage(null, msg0);
        myCursor.addMessageLast(msg0);
        msg0.decrementReferenceCount();
        if(myCursor.hasNext()) {
            MessageReference ref = myCursor.next();
            LOG.info("Received message: {} with body: ({})", ref.getMessageId(), ((ActiveMQTextMessage)ref.getMessage()).getText());


            //simulate send ack to store to remove message
            myCursor.remove();
            try {
                queueMessageStore.removeMessage(ref.getMessageId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // simulate full memory (from other resources) such that caching stops
        systemUsage.getMemoryUsage().increaseUsage(messageBytesSize * 10);

        ActiveMQTextMessage msg1 = getMessage(1);
        msg1.setMemoryUsage(systemUsage.getMemoryUsage());
        queueMessageStore.addMessage(null, msg1);
        myCursor.addMessageLast(msg1);
        msg1.decrementReferenceCount();

        boolean b = true;
        while (b) {
            if(myCursor.hasNext()) {
                MessageReference ref = myCursor.next();
                LOG.info("Received message: {} with body: ({})", ref.getMessageId(), ((ActiveMQTextMessage)ref.getMessage()).getText());

                //simulate send ack to store to remove message
                myCursor.remove();
                try {
                    queueMessageStore.removeMessage(ref.getMessageId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                b = false;
            }
        }
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
