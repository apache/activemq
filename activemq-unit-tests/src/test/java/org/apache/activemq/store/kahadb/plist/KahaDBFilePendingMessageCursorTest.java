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
package org.apache.activemq.store.kahadb.plist;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.*;
import org.apache.activemq.broker.region.cursors.FilePendingMessageCursor;
import org.apache.activemq.broker.region.cursors.FilePendingMessageCursorTestSupport;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.usage.SystemUsage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class KahaDBFilePendingMessageCursorTest extends FilePendingMessageCursorTestSupport {

    @Test
    public void testAddRemoveAddIndexSize() throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getMemoryUsage().setLimit(1024*150);
        String body = new String(new byte[1024]);
        Destination destination = new Queue(brokerService, new ActiveMQQueue("Q"), null, new DestinationStatistics(), null);

        brokerService.start();
        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        LOG.info("start");
        final PageFile pageFile =  ((PListImpl)underTest.getDiskList()).getPageFile();
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
                ref.decrementReferenceCount();
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

    // Test for AMQ-9726
    @Test
    public void testClearCursor() throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getMemoryUsage().setLimit(1024*150);
        Destination dest = new Queue(brokerService, new ActiveMQQueue("Q"), null, new DestinationStatistics(), null);
        dest.setMemoryUsage(usage.getMemoryUsage());
        brokerService.start();

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // Add 10 messages to the cursor in memory
        addTestMessages(dest);

        // Verify memory usage was increased and cache is enabled
        assertTrue(dest.getMemoryUsage().getUsage() > 0);
        assertEquals(10, underTest.size());
        assertTrue(underTest.isCacheEnabled());
        assertEquals(0, dest.getTempUsage().getUsage());

        // Clear, this will verify memory usage is correctly decremented
        // and the memory map is cleared as well. Memory was previously
        // incorrectly not being cleared.
        underTest.clear();
        assertEquals(0, underTest.size());
        assertEquals(0, dest.getMemoryUsage().getUsage());

        // Now test the disk cursor
        // set the memory usage limit very small so messages will go to
        // the disk list and not memory and send 10 more messages
        usage.getMemoryUsage().setLimit(1);
        addTestMessages(dest);

        // confirm the cache is false and the memory is 0 because
        // the messages exist on disk and not in the memory map
        // also very temp usage is greater than 0 now
        assertFalse(underTest.isCacheEnabled());
        assertEquals(0, dest.getMemoryUsage().getUsage());
        assertTrue(dest.getTempUsage().getUsage() > 0);
        assertEquals(10, underTest.size());

        // Test clearing the disk list shows a size of 0
        underTest.clear();
        assertEquals(0, underTest.size());

        // Send 10 more messages to verify that we can send again
        // to the disk list after clear. Previously clear did not
        // correctly destroy/reset the disk cursor so an exception
        // was thrown when adding messages again after calling clear()
        addTestMessages(dest);
        assertFalse(underTest.isCacheEnabled());
        assertEquals(0, dest.getMemoryUsage().getUsage());
        assertTrue(dest.getTempUsage().getUsage() > 0);
        assertEquals(10, underTest.size());

        // one final clear() and reset limit to make sure we can send to
        // memory again
        underTest.clear();
        usage.getMemoryUsage().setLimit(1024*150);
        assertEquals(0, underTest.size());
        assertEquals(0, dest.getMemoryUsage().getUsage());

        // Verify memory usage was increased and cache is enabled
        addTestMessages(dest);
        assertTrue(dest.getMemoryUsage().getUsage() > 0);
        assertEquals(10, underTest.size());
        assertTrue(underTest.isCacheEnabled());
    }

    private void addTestMessages(Destination dest) throws Exception {
        for (int i = 0; i< 10; i++) {
            ActiveMQMessage mqMessage = new ActiveMQMessage();
            mqMessage.setMessageId(new MessageId("1:2:3:" + i));
            mqMessage.setMemoryUsage(dest.getMemoryUsage());
            mqMessage.setRegionDestination(dest);
            underTest.addMessageLast(new IndirectMessageReference(mqMessage));
        }
    }

}
