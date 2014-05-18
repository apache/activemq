package org.apache.activemq.broker.region.virtual;

import java.util.Set;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.SubscriptionKey;

/**
 * Creates a mapped Queue that can recover messages from subscription recovery policy of its Virtual Topic.
 *
 * @author Dhiraj Bokde
 */
public class MappedQueueFilter extends DestinationFilter {

    private final ActiveMQDestination virtualDestination;

    public MappedQueueFilter(ActiveMQDestination virtualDestination, Destination destination) {
        super(destination);
        this.virtualDestination = virtualDestination;
    }

    @Override
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        // recover messages for first consumer only
        boolean noSubs, empty;
        synchronized (this) {
            noSubs = getConsumers().isEmpty();
            super.addSubscription(context, sub);
            empty = getConsumers().isEmpty();
        }

        if (noSubs && !empty) {
            // new subscription added, recover retroactive messages
            final RegionBroker regionBroker = (RegionBroker) context.getBroker().getAdaptor(RegionBroker.class);
            final Set<Destination> virtualDests = regionBroker.getDestinations(virtualDestination);

            final ActiveMQDestination newDestination = sub.getActiveMQDestination();
            final BaseDestination regionDest = getBaseDestination((Destination)
                regionBroker.getDestinations(newDestination).toArray()[0]);

            for (Destination virtualDest : virtualDests) {
                if (virtualDest.getActiveMQDestination().isTopic() &&
                    (virtualDest.isAlwaysRetroactive() || sub.getConsumerInfo().isRetroactive())) {
                    Topic topic = (Topic) getBaseDestination(virtualDest);
                    if (topic != null) {
                        // re-use browse() to get recovered messages
                        final Message[] messages = topic.getSubscriptionRecoveryPolicy().browse(
                            topic.getActiveMQDestination());

                        // add recovered messages to subscription
                        for (Message message : messages) {
                            final Message copy = message.copy();
                            copy.setOriginalDestination(message.getDestination());
                            copy.setDestination(newDestination);
                            copy.setRegionDestination(regionDest);
                            sub.addRecoveredMessage(context,
                                newDestination.isQueue() ? new IndirectMessageReference(copy) : copy);
                        }
                    }
                }
            }
        }
    }

    private BaseDestination getBaseDestination(Destination virtualDest) {
        if (virtualDest instanceof BaseDestination) {
            return (BaseDestination) virtualDest;
        } else if (virtualDest instanceof DestinationFilter) {
            return ((DestinationFilter)virtualDest).getAdaptor(BaseDestination.class);
        }
        return null;
    }

    @Override
    public synchronized void removeSubscription(ConnectionContext context,
                                                Subscription sub, long lastDeliveredSequenceId) throws Exception {
        super.removeSubscription(context, sub, lastDeliveredSequenceId);
    }

    @Override
    public synchronized void deleteSubscription(ConnectionContext context, SubscriptionKey key) throws Exception {
        super.deleteSubscription(context, key);
    }

}
