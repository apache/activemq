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
