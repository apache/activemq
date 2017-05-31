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

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IdGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test is for AMQ-5748 to verify that {@link MessageStore} implements correctly
 * compute the size of the messages in the store.
 *
 */
public abstract class AbstractMessageStoreSizeTest {

    protected static final IdGenerator id = new IdGenerator();
    protected ActiveMQQueue destination = new ActiveMQQueue("Test");
    protected ProducerId producerId = new ProducerId("1.1.1");
    protected static final int MESSAGE_COUNT = 20;
    protected static String dataDirectory = "target/test-amq-5748/datadb";
    protected static int testMessageSize = 1000;

    @Before
    public void init() throws Exception {
        this.initStore();
    }

    @After
    public void destroy() throws Exception {
        this.destroyStore();
    }

    protected abstract void initStore() throws Exception;


    protected abstract void destroyStore() throws Exception;


    /**
     * This method tests that the message size exists after writing a bunch of messages to the store.
     * @throws Exception
     */
    @Test
    public void testMessageSize() throws Exception {
        writeMessages();
        long messageSize = getMessageStore().getMessageSize();
        assertTrue(getMessageStore().getMessageCount() == 20);
        assertTrue(messageSize > 20 * testMessageSize);
    }


    /**
     * Write random byte messages to the store for testing.
     *
     * @throws Exception
     */
    protected void writeMessages() throws Exception {
        final ConnectionContext context = new ConnectionContext();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            ActiveMQMessage message = new ActiveMQMessage();
            final byte[] data = new byte[testMessageSize];
            final Random rng = new Random();
            rng.nextBytes(data);
            message.setContent(new ByteSequence(data));
            message.setDestination(destination);
            message.setMessageId(new MessageId(id.generateId() + ":1", i));
            getMessageStore().addMessage(context, message);
        }
    }

    protected abstract MessageStore getMessageStore();
}
