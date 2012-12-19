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
package org.apache.activemq.store;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class PersistenceAdapterTestSupport extends TestCase {

    protected PersistenceAdapter pa;
    protected BrokerService brokerService;

    abstract protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception;

    @Override
    protected void setUp() throws Exception {
        brokerService = new BrokerService();
        pa = createPersistenceAdapter(true);
        brokerService.setPersistenceAdapter(pa);
        brokerService.start();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }
    
    public void testStoreCanHandleDupMessages() throws Exception {

        
        MessageStore ms = pa.createQueueMessageStore(new ActiveMQQueue("TEST"));
        ConnectionContext context = new ConnectionContext();

        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("test");
        MessageId messageId = new MessageId("ID:localhost-56913-1254499826208-0:0:1:1:1");
        messageId.setBrokerSequenceId(1);
        message.setMessageId(messageId);
        ms.addMessage(context, message);

        // here comes the dup...
        message = new ActiveMQTextMessage();
        message.setText("test");
        messageId = new MessageId("ID:localhost-56913-1254499826208-0:0:1:1:1");
        messageId.setBrokerSequenceId(2);
        message.setMessageId(messageId);
        ms.addMessage(context, message);

        final AtomicInteger recovered = new AtomicInteger();
        ms.recover(new MessageRecoveryListener() {
            public boolean hasSpace() {
                return true;
            }

            public boolean isDuplicate(MessageId ref) {
                return false;
            }

            public boolean recoverMessage(Message message) throws Exception {
                recovered.incrementAndGet();
                return true;
            }

            public boolean recoverMessageReference(MessageId ref) throws Exception {
                recovered.incrementAndGet();
                return true;
            }
        });
        assertEquals(1, recovered.get());

    }

    public void testAddRemoveConsumerDest() throws Exception {
        ActiveMQQueue consumerQ = new ActiveMQQueue("Consumer.A.VirtualTopicTest");
        MessageStore ms = pa.createQueueMessageStore(consumerQ);
        pa.removeQueueMessageStore(consumerQ);
        assertFalse(pa.getDestinations().contains(consumerQ));
    }

}
