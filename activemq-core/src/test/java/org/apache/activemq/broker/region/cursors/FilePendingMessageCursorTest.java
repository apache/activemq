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
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.kahadb.plist.PList;
import org.apache.activemq.usage.SystemUsage;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.util.ByteSequence;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilePendingMessageCursorTest {
    private static final Logger LOG = LoggerFactory.getLogger(FilePendingMessageCursorTest.class);
    BrokerService brokerService;
    FilePendingMessageCursor underTest;

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.getTempDataStore().stop();
        }
    }

    private void createBrokerWithTempStoreLimit() throws Exception {
        brokerService = new BrokerService();
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getTempUsage().setLimit(1025*1024*15);

        // put something in the temp store to on demand initialise it
        PList dud = brokerService.getTempDataStore().getPList("dud");
        dud.addFirst("A", new ByteSequence("A".getBytes()));
    }

    @Test
    public void testAddToEmptyCursorWhenTempStoreIsFull() throws Exception {
        createBrokerWithTempStoreLimit();
        SystemUsage usage = brokerService.getSystemUsage();
        assertTrue("temp store is full: %" + usage.getTempUsage().getPercentUsage(), usage.getTempUsage().isFull());

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // ok to add
        underTest.addMessageLast(QueueMessageReference.NULL_MESSAGE);

        assertFalse("cursor is not full", underTest.isFull());
    }

    @Test
    public void testAddRemoveAddIndexSize() throws Exception {
        brokerService = new BrokerService();
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getMemoryUsage().setLimit(1024*150);
        String body = new String(new byte[1024]);
        Destination destination = new Queue(brokerService, new ActiveMQQueue("Q"), null, new DestinationStatistics(), null);

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        LOG.info("start");
        final PageFile pageFile =  underTest.getDiskList().getPageFile();
        LOG.info("page count: " +pageFile.getPageCount());
        LOG.info("free count: " + pageFile.getFreePageCount());
        LOG.info("content size: " +pageFile.getPageContentSize());

        final long initialPageCount =  pageFile.getPageCount();

        final int numMessages = 1000;

        for (int j=0; j<10; j++) {
            // ensure free pages are reused
            for (int i=0; i< numMessages; i++) {
                ActiveMQMessage mqMessage = new ActiveMQMessage();
                mqMessage.setStringProperty("body", body);
                mqMessage.setMessageId(new MessageId("1:2:3:" + i));
                mqMessage.setMemoryUsage(usage.getMemoryUsage());
                mqMessage.setRegionDestination(destination);
                underTest.addMessageLast(new IndirectMessageReference(mqMessage));
            }
            assertFalse("cursor is not full " + usage.getTempUsage(), underTest.isFull());

            underTest.reset();
            long receivedCount = 0;
            while(underTest.hasNext()) {
                MessageReference ref = underTest.next();
                underTest.remove();
                assertEquals("id is correct", receivedCount++, ref.getMessageId().getProducerSequenceId());
            }
            assertEquals("got all messages back", receivedCount, numMessages);
            LOG.info("page count: " +pageFile.getPageCount());
            LOG.info("free count: " + pageFile.getFreePageCount());
            LOG.info("content size: " + pageFile.getPageContentSize());
        }

        assertEquals("expected page usage", initialPageCount, pageFile.getPageCount() - pageFile.getFreePageCount() );

        LOG.info("Destroy");
        underTest.destroy();
        LOG.info("page count: " + pageFile.getPageCount());
        LOG.info("free count: " + pageFile.getFreePageCount());
        LOG.info("content size: " + pageFile.getPageContentSize());
        assertEquals("expected page usage", initialPageCount -1, pageFile.getPageCount() - pageFile.getFreePageCount() );
    }
}
