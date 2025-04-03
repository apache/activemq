package org.apache.activemq.broker.region;

import jakarta.jms.InvalidSelectorException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.usage.UsageListener;

public class SharedTopicSubscription extends AbstractSubscription {

    public SharedTopicSubscription(Broker broker, ConnectionContext context,
            ConsumerInfo info) throws InvalidSelectorException {
        super(broker, context, info);
    }

    @Override
    public void add(MessageReference node) throws Exception {

    }

    @Override
    public Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
        return null;
    }

    @Override
    public void processMessageDispatchNotification(MessageDispatchNotification mdn) throws Exception {

    }

    @Override
    public int getPendingQueueSize() {
        return 0;
    }

    @Override
    public long getPendingMessageSize() {
        return 0;
    }

    @Override
    public int getDispatchedQueueSize() {
        return 0;
    }

    @Override
    public long getDispatchedCounter() {
        return 0;
    }

    @Override
    public long getEnqueueCounter() {
        return 0;
    }

    @Override
    public long getDequeueCounter() {
        return 0;
    }

    @Override
    public boolean isLowWaterMark() {
        return false;
    }

    @Override
    public boolean isHighWaterMark() {
        return false;
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public void updateConsumerPrefetch(int newPrefetch) {

    }

    @Override
    public void destroy() {

    }

    @Override
    public int getInFlightSize() {
        return 0;
    }
}
