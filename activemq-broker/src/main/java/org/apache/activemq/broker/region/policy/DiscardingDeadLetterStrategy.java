package org.apache.activemq.broker.region.policy;

import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DeadLetterStrategy} where each destination has its own individual
 * DLQ using the subject naming hierarchy.
 *
 * @org.apache.xbean.XBean element="discarding" description="Dead Letter Strategy that discards all messages"
 *
 */
public class DiscardingDeadLetterStrategy extends SharedDeadLetterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DiscardingDeadLetterStrategy.class);

    @Override
    public boolean isSendToDeadLetterQueue(Message message) {
        boolean result = false;
        LOG.debug("Discarding message sent to DLQ: {}, dest: {}", message.getMessageId(), message.getDestination());
        return result;
    }
}
