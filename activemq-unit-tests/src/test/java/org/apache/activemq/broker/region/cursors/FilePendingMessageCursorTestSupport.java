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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.PList;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class FilePendingMessageCursorTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(FilePendingMessageCursorTestSupport.class);
    protected BrokerService brokerService;
    protected FilePendingMessageCursor underTest;
    private final AtomicInteger discarded = new AtomicInteger();
    private final AtomicInteger dlq = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        discarded.set(0);
        dlq.set(0);
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    private void createBrokerWithTempStoreLimit() throws Exception {
        createBrokerWithTempStoreLimit(1025*1024*15);
    }

    private void createBrokerWithTempStoreLimit(long limit) throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getTempUsage().setLimit(limit);
        brokerService.setPlugins(new BrokerPlugin[] { new BrokerPluginSupport() {
            @Override
            public void messageDiscarded(ConnectionContext context, Subscription sub,
                    MessageReference messageReference) {
                super.messageDiscarded(context, sub, messageReference);
                discarded.incrementAndGet();
            }

            @Override
            public boolean sendToDeadLetterQueue(ConnectionContext context,
                    MessageReference messageReference, Subscription subscription,
                    Throwable poisonCause) {
                dlq.incrementAndGet();
                return super.sendToDeadLetterQueue(context, messageReference, subscription,
                        poisonCause);
            }
        }});
        brokerService.start();

        // put something in the temp store to on demand initialise it
        PList dud = brokerService.getTempDataStore().getPList("dud");
        dud.addFirst("A", new ByteSequence("A".getBytes()));
    }

    @Test
    public void testAddToEmptyCursorWhenTempStoreIsFull() throws Exception {
        createBrokerWithTempStoreLimit();
        SystemUsage usage = brokerService.getSystemUsage();

        PList dud = brokerService.getTempDataStore().getPList("dud");
        // fill the temp store
        int id=0;
        ByteSequence payload = new ByteSequence(new byte[1024]);
        while (!usage.getTempUsage().isFull()) {
            dud.addFirst("A-" + (++id), payload);
        }

        assertTrue("temp store is full: %" + usage.getTempUsage().getPercentUsage(), usage.getTempUsage().isFull());

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // ok to add
        underTest.addMessageLast(QueueMessageReference.NULL_MESSAGE);

        assertFalse("cursor is not full", underTest.isFull());
    }

    @Test
    public void testResetClearsIterator() throws Exception {
        createBrokerWithTempStoreLimit();

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        // ok to add
        underTest.addMessageLast(QueueMessageReference.NULL_MESSAGE);

        underTest.reset();
        underTest.release();

        try {
            underTest.hasNext();
            fail("expect npe on use of iterator after release");
        } catch (NullPointerException expected) {}
    }

    // This test will verify that when flushing message in memory to the
    // temp store that the store will check usage and not keep writing when full
    // If full, remaining messages will just get dropped
    @Test(timeout=30000)
    public void testFlushToDiskWhenTempStoreIsFull() throws Exception {
        createBrokerWithTempStoreLimit(1024 * 1024);
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getMemoryUsage().setLimit(1024 * 100);
        Queue destination = new Queue(brokerService, new ActiveMQQueue("Q"), null, new DestinationStatistics(), null);
        destination.setAdvisoryForDiscardingMessages(true);
        PList diskList = brokerService.getTempDataStore().getPList("temp");

        // fill the temp store
        int id = 0;
        ByteSequence payload = new ByteSequence(new byte[1024]);
        while (!usage.getTempUsage().isFull()) {
            diskList.addLast("A-" + (++id), payload);
        }

        // Ensure that the temp store is full and get current usage
        TempUsage tempUsage = usage.getTempUsage();
        long existingUsage = tempUsage.getUsage();
        assertTrue("temp store is full: %" + tempUsage.getPercentUsage(), tempUsage.isFull());

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // At this point the memory cursor is empty
        // This will write messages until it fills and then the cursor will try
        // and dump to disk. All messages will just get dropped because there's no space.
        int i = 0;
        ActiveMQMessage mqMessage;
        do {
            mqMessage = new ActiveMQMessage();
            mqMessage.setMessageId(new MessageId("1:2:3:" + i++));
            mqMessage.setContent(new ByteSequence(new byte[1024]));
            mqMessage.setRegionDestination(destination);
            mqMessage.setMemoryUsage(usage.getMemoryUsage());
        // This should timeout and return false when trying to write to disk because
        // the cursor now checks if temp is full before trying to write
        } while(underTest.tryAddMessageLast(new IndirectMessageReference(mqMessage), 10));

        // Verify that the usage didn't increase, and no messages were written to
        // temp because it was already full
        assertEquals(existingUsage, tempUsage.getUsage());
        assertEquals(0, underTest.getDiskList().size());
        // memory usage should be 0 because all the messages get dropped
        assertEquals(0, usage.getMemoryUsage().getUsage());
        // the discard callback should have been called for all the messages that
        // were removed from the memory store because temp was full. This is
        // i - 1 because the last message failed to process and timed out so it
        // was not already in the memory store and was not discarded.
        assertEquals(i - 1, discarded.get());

        // all messages should have been sent for possible DLQ processing
        // the messages are non-persistent so the callback would generally skip sending
        // them to the DLQ because processNonPersistent is usually false, but this verifies
        // the callback is at least called in case someone has enabled the flag
        assertEquals(discarded.get(), dlq.get());
    }

    @Test(timeout=30000)
    public void testFlushToDiskWhenTempStoreIsHalfFull() throws Exception {
        createBrokerWithTempStoreLimit(1024 * 512);
        SystemUsage usage = brokerService.getSystemUsage();
        // set a memory usage higher than on disk limit for testing
        usage.getMemoryUsage().setLimit(1024 * 1024);
        Queue destination = new Queue(brokerService, new ActiveMQQueue("Q"), null, new DestinationStatistics(), null);
        destination.setAdvisoryForDiscardingMessages(true);
        PList diskList = brokerService.getTempDataStore().getPList("temp");

        // fill the temp store halfway
        int id = 0;
        ByteSequence payload = new ByteSequence(new byte[1024]);
        while (!usage.getTempUsage().isFull(50)) {
            diskList.addLast("A-" + (++id), payload);
        }

        // Ensure that the temp store is half full and get current usage
        TempUsage tempUsage = usage.getTempUsage();
        assertTrue("temp store is full: %" + tempUsage.getPercentUsage(), tempUsage.isFull(50));

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // Start adding messages
        int i = 0;
        ActiveMQMessage mqMessage;
        do {
            mqMessage = new ActiveMQMessage();
            mqMessage.setMessageId(new MessageId("1:2:3:" + i++));
            mqMessage.setContent(new ByteSequence(new byte[1024]));
            mqMessage.setMemoryUsage(usage.getMemoryUsage());
            mqMessage.setRegionDestination(destination);
            // This should timeout and return false when trying to write to disk when Temp is full
            // and will cause the loop to stop
        } while(underTest.tryAddMessageLast(new IndirectMessageReference(mqMessage), 10));

        // This should be false, there was not enough space for all the messages in memory but for some
        // so the dumping from memory to disk should have written part of the messages
        assertFalse(underTest.getDiskList().isEmpty());
        // we should have discarded the remaining from memory
        assertTrue(discarded.get() > 0);
        // This is i - 1 because the last message failed to add on timeout.
        // This verifies that the total messages added equals the number on disk + discarded
        // because any excess now get discarded when temp store is full
        assertEquals(i - 1, underTest.getDiskList().size() + discarded.get());
        // memory usage should be 0 because all the messages get moved from memory
        assertEquals(0, usage.getMemoryUsage().getUsage());
        // discarded should equal possible DLQ
        assertEquals(discarded.get(), dlq.get());
    }

    @Test(timeout=30000)
    public void testFlushToDiskWhenTempStoreHasSpace() throws Exception {
        // create with plenty of space
        createBrokerWithTempStoreLimit(10 * 1024 * 1024);
        SystemUsage usage = brokerService.getSystemUsage();
        // set a memory usage lower than disk limit
        usage.getMemoryUsage().setLimit(1024 * 1024);
        Queue destination = new Queue(brokerService, new ActiveMQQueue("Q"), null, new DestinationStatistics(), null);
        destination.setAdvisoryForDiscardingMessages(true);
        PList diskList = brokerService.getTempDataStore().getPList("temp");

        // fill the temp store only 10% which gives plenty of space for messages later
        int id = 0;
        ByteSequence payload = new ByteSequence(new byte[1024]);
        while (!usage.getTempUsage().isFull(10)) {
            diskList.addLast("A-" + (++id), payload);
        }
        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // Start adding messages
        int i = 0;
        ActiveMQMessage mqMessage;
        do {
            mqMessage = new ActiveMQMessage();
            mqMessage.setMessageId(new MessageId("1:2:3:" + i++));
            mqMessage.setContent(new ByteSequence(new byte[1024]));
            mqMessage.setMemoryUsage(usage.getMemoryUsage());
            mqMessage.setRegionDestination(destination);
            // Keep writing until disk list is no longer null which means we now dumped to disk
            // because memory is full
        } while(underTest.tryAddMessageLast(new IndirectMessageReference(mqMessage), 10)
                && underTest.getDiskList() != null);

        // We had space so every message sent should exist on disk
        assertEquals(i - 1, underTest.getDiskList().size());
        // discarded/dlq should be 0
        assertEquals(0, discarded.get());
        assertEquals(0, dlq.get());
        // memory usage should be 0 because all the messages get moved from memory
        assertEquals(0, usage.getMemoryUsage().getUsage());
    }
}
