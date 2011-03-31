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
package org.apache.activemq.broker.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.JMSException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.DestinationAlreadyExistsException;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public abstract class AbstractRegion implements Region {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRegion.class);

    protected final Map<ActiveMQDestination, Destination> destinations = new ConcurrentHashMap<ActiveMQDestination, Destination>();
    protected final DestinationMap destinationMap = new DestinationMap();
    protected final Map<ConsumerId, Subscription> subscriptions = new ConcurrentHashMap<ConsumerId, Subscription>();
    protected final SystemUsage usageManager;
    protected final DestinationFactory destinationFactory;
    protected final DestinationStatistics destinationStatistics;
    protected final RegionBroker broker;
    protected boolean autoCreateDestinations = true;
    protected final TaskRunnerFactory taskRunnerFactory;
    protected final Object destinationsMutex = new Object();
    protected final Map<ConsumerId, Object> consumerChangeMutexMap = new HashMap<ConsumerId, Object>();
    protected boolean started;

    public AbstractRegion(RegionBroker broker, DestinationStatistics destinationStatistics, SystemUsage memoryManager,
            TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        if (broker == null) {
            throw new IllegalArgumentException("null broker");
        }
        this.broker = broker;
        this.destinationStatistics = destinationStatistics;
        this.usageManager = memoryManager;
        this.taskRunnerFactory = taskRunnerFactory;
        if (broker == null) {
            throw new IllegalArgumentException("null destinationFactory");
        }
        this.destinationFactory = destinationFactory;
    }

    public final void start() throws Exception {
        started = true;

        Set<ActiveMQDestination> inactiveDests = getInactiveDestinations();
        for (Iterator<ActiveMQDestination> iter = inactiveDests.iterator(); iter.hasNext();) {
            ActiveMQDestination dest = iter.next();

            ConnectionContext context = new ConnectionContext();
            context.setBroker(broker.getBrokerService().getBroker());
            context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
            context.getBroker().addDestination(context, dest, false);
        }
        synchronized (destinationsMutex) {
            for (Iterator<Destination> i = destinations.values().iterator(); i.hasNext();) {
                Destination dest = i.next();
                dest.start();
            }
        }
    }

    public void stop() throws Exception {
        started = false;
        synchronized (destinationsMutex) {
            for (Iterator<Destination> i = destinations.values().iterator(); i.hasNext();) {
                Destination dest = i.next();
                dest.stop();
            }
        }
        destinations.clear();
    }

    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,
            boolean createIfTemporary) throws Exception {
        LOG.debug(broker.getBrokerName() + " adding destination: " + destination);
        synchronized (destinationsMutex) {
            Destination dest = destinations.get(destination);
            if (dest == null) {
                if (destination.isTemporary() == false || createIfTemporary) {
                    dest = createDestination(context, destination);
                    // intercept if there is a valid interceptor defined
                    DestinationInterceptor destinationInterceptor = broker.getDestinationInterceptor();
                    if (destinationInterceptor != null) {
                        dest = destinationInterceptor.intercept(dest);
                    }
                    dest.start();
                    destinations.put(destination, dest);
                    destinationMap.put(destination, dest);
                    addSubscriptionsForDestination(context, dest);
                    if (destination.isTemporary()) {
                        // need to associate with the connection so it can get removed
                        if (context.getConnection() instanceof TransportConnection) {
                            TransportConnection transportConnection = (TransportConnection) context.getConnection();
                            DestinationInfo info = new DestinationInfo(context.getConnectionId(),
                                    DestinationInfo.ADD_OPERATION_TYPE,
                                    destination);
                            transportConnection.processAddDestination(info);
                            LOG.debug("assigning ownership of auto created temp : " + destination + " to connection:"
                                    + context.getConnectionId());
                        }
                    }
                }
                if (dest == null) {
                    throw new JMSException("The destination " + destination + " does not exist.");
                }
            }
            return dest;
        }
    }

    public Map<ConsumerId, Subscription> getSubscriptions() {
        return subscriptions;
    }

    protected List<Subscription> addSubscriptionsForDestination(ConnectionContext context, Destination dest)
            throws Exception {

        List<Subscription> rc = new ArrayList<Subscription>();
        // Add all consumers that are interested in the destination.
        for (Iterator<Subscription> iter = subscriptions.values().iterator(); iter.hasNext();) {
            Subscription sub = iter.next();
            if (sub.matches(dest.getActiveMQDestination())) {
                dest.addSubscription(context, sub);
                rc.add(sub);
            }
        }
        return rc;

    }

    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout)
            throws Exception {

        // No timeout.. then try to shut down right way, fails if there are
        // current subscribers.
        if (timeout == 0) {
            for (Iterator<Subscription> iter = subscriptions.values().iterator(); iter.hasNext();) {
                Subscription sub = iter.next();
                if (sub.matches(destination)) {
                    throw new JMSException("Destination still has an active subscription: " + destination);
                }
            }
        }

        if (timeout > 0) {
            // TODO: implement a way to notify the subscribers that we want to
            // take the down
            // the destination and that they should un-subscribe.. Then wait up
            // to timeout time before
            // dropping the subscription.
        }

        LOG.debug("Removing destination: " + destination);

        synchronized (destinationsMutex) {
            Destination dest = destinations.remove(destination);
            if (dest != null) {
                // timeout<0 or we timed out, we now force any remaining
                // subscriptions to un-subscribe.
                for (Iterator<Subscription> iter = subscriptions.values().iterator(); iter.hasNext();) {
                    Subscription sub = iter.next();
                    if (sub.matches(destination)) {
                        dest.removeSubscription(context, sub, 0l);
                    }
                }
                destinationMap.removeAll(destination);
                dispose(context, dest);
                DestinationInterceptor destinationInterceptor = broker.getDestinationInterceptor();
                if (destinationInterceptor != null) {
                    destinationInterceptor.remove(dest);
                }

            } else {
                LOG.debug("Destination doesn't exist: " + dest);
            }
        }
    }

    /**
     * Provide an exact or wildcard lookup of destinations in the region
     * 
     * @return a set of matching destination objects.
     */
    public Set<Destination> getDestinations(ActiveMQDestination destination) {
        synchronized (destinationsMutex) {
            return destinationMap.get(destination);
        }
    }

    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        synchronized (destinationsMutex) {
            return new HashMap<ActiveMQDestination, Destination>(destinations);
        }
    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        LOG.debug(broker.getBrokerName() + " adding consumer: " + info.getConsumerId() + " for destination: "
                + info.getDestination());
        ActiveMQDestination destination = info.getDestination();
        if (destination != null && !destination.isPattern() && !destination.isComposite()) {
            // lets auto-create the destination
            lookup(context, destination,true);
        }

        Object addGuard;
        synchronized (consumerChangeMutexMap) {
            addGuard = consumerChangeMutexMap.get(info.getConsumerId());
            if (addGuard == null) {
                addGuard = new Object();
                consumerChangeMutexMap.put(info.getConsumerId(), addGuard);
            }
        }
        synchronized (addGuard) {
            Subscription o = subscriptions.get(info.getConsumerId());
            if (o != null) {
                LOG
                        .warn("A duplicate subscription was detected. Clients may be misbehaving. Later warnings you may see about subscription removal are a consequence of this.");
                return o;
            }

            // We may need to add some destinations that are in persistent store
            // but not active
            // in the broker.
            //
            // TODO: think about this a little more. This is good cause
            // destinations are not loaded into
            // memory until a client needs to use the queue, but a management
            // agent viewing the
            // broker will not see a destination that exists in persistent
            // store. We may want to
            // eagerly load all destinations into the broker but have an
            // inactive state for the
            // destination which has reduced memory usage.
            //
            DestinationFilter.parseFilter(info.getDestination());

            Subscription sub = createSubscription(context, info);

            subscriptions.put(info.getConsumerId(), sub);

            // At this point we're done directly manipulating subscriptions,
            // but we need to retain the synchronized block here. Consider
            // otherwise what would happen if at this point a second
            // thread added, then removed, as would be allowed with
            // no mutex held. Remove is only essentially run once
            // so everything after this point would be leaked.

            // Add the subscription to all the matching queues.
            // But copy the matches first - to prevent deadlocks
            List<Destination> addList = new ArrayList<Destination>();
            synchronized (destinationsMutex) {
                for (Iterator iter = destinationMap.get(info.getDestination()).iterator(); iter.hasNext();) {
                    Destination dest = (Destination) iter.next();
                    addList.add(dest);
                }
            }

            for (Destination dest : addList) {
                dest.addSubscription(context, sub);
            }

            if (info.isBrowser()) {
                ((QueueBrowserSubscription) sub).destinationsAdded();
            }

            return sub;
        }
    }

    /**
     * Get all the Destinations that are in storage
     * 
     * @return Set of all stored destinations
     */
    public Set getDurableDestinations() {
        return destinationFactory.getDestinations();
    }

    /**
     * @return all Destinations that don't have active consumers
     */
    protected Set<ActiveMQDestination> getInactiveDestinations() {
        Set<ActiveMQDestination> inactiveDests = destinationFactory.getDestinations();
        synchronized (destinationsMutex) {
            inactiveDests.removeAll(destinations.keySet());
        }
        return inactiveDests;
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        LOG.debug(broker.getBrokerName() + " removing consumer: " + info.getConsumerId() + " for destination: "
                + info.getDestination());

        Subscription sub = subscriptions.remove(info.getConsumerId());
        // The sub could be removed elsewhere - see ConnectionSplitBroker
        if (sub != null) {

            // remove the subscription from all the matching queues.
            List<Destination> removeList = new ArrayList<Destination>();
            synchronized (destinationsMutex) {
                for (Iterator iter = destinationMap.get(info.getDestination()).iterator(); iter.hasNext();) {
                    Destination dest = (Destination) iter.next();
                    removeList.add(dest);

                }
            }
            for (Destination dest : removeList) {
                dest.removeSubscription(context, sub, info.getLastDeliveredSequenceId());
            }

            destroySubscription(sub);
        }
        synchronized (consumerChangeMutexMap) {
            consumerChangeMutexMap.remove(info.getConsumerId());
        }
    }

    protected void destroySubscription(Subscription sub) {
        sub.destroy();
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        throw new JMSException("Invalid operation.");
    }

    public void send(final ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();

        if (producerExchange.isMutable() || producerExchange.getRegionDestination() == null) {
            final Destination regionDestination = lookup(context, messageSend.getDestination(),false);
            producerExchange.setRegionDestination(regionDestination);
        }

        producerExchange.getRegionDestination().send(producerExchange, messageSend);
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        Subscription sub = consumerExchange.getSubscription();
        if (sub == null) {
            sub = subscriptions.get(ack.getConsumerId());
            if (sub == null) {
                if (!consumerExchange.getConnectionContext().isInRecoveryMode()) {
                    LOG.warn("Ack for non existent subscription, ack:" + ack);
                    throw new IllegalArgumentException("The subscription does not exist: " + ack.getConsumerId());
                } else {
                    LOG.debug("Ack for non existent subscription in recovery, ack:" + ack);
                    return;
                }
            }
            consumerExchange.setSubscription(sub);
        }
        sub.acknowledge(consumerExchange.getConnectionContext(), ack);
    }

    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        Subscription sub = subscriptions.get(pull.getConsumerId());
        if (sub == null) {
            throw new IllegalArgumentException("The subscription does not exist: " + pull.getConsumerId());
        }
        return sub.pullMessage(context, pull);
    }

    protected Destination lookup(ConnectionContext context, ActiveMQDestination destination,boolean createTemporary) throws Exception {
        Destination dest = null;
        synchronized (destinationsMutex) {
            dest = destinations.get(destination);
        }
        if (dest == null) {
            if (isAutoCreateDestinations()) {
                // Try to auto create the destination... re-invoke broker
                // from the
                // top so that the proper security checks are performed.
                try {
                    context.getBroker().addDestination(context, destination, createTemporary);
                    dest = addDestination(context, destination, false);
                } catch (DestinationAlreadyExistsException e) {
                    // if the destination already exists then lets ignore
                    // this error
                }
                // We should now have the dest created.
                synchronized (destinationsMutex) {
                    dest = destinations.get(destination);
                }
            }
            if (dest == null) {
                throw new JMSException("The destination " + destination + " does not exist.");
            }
        }
        return dest;
    }

    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        Subscription sub = subscriptions.get(messageDispatchNotification.getConsumerId());
        if (sub != null) {
            sub.processMessageDispatchNotification(messageDispatchNotification);
        } else {
            throw new JMSException("Slave broker out of sync with master - Subscription: "
                    + messageDispatchNotification.getConsumerId() + " on "
                    + messageDispatchNotification.getDestination() + " does not exist for dispatch of message: "
                    + messageDispatchNotification.getMessageId());
        }
    }

    /*
     * For a Queue/TempQueue, dispatch order is imperative to match acks, so the
     * dispatch is deferred till the notification to ensure that the
     * subscription chosen by the master is used. AMQ-2102
     */
    protected void processDispatchNotificationViaDestination(MessageDispatchNotification messageDispatchNotification)
            throws Exception {
        Destination dest = null;
        synchronized (destinationsMutex) {
            dest = destinations.get(messageDispatchNotification.getDestination());
        }
        if (dest != null) {
            dest.processDispatchNotification(messageDispatchNotification);
        } else {
            throw new JMSException("Slave broker out of sync with master - Destination: "
                    + messageDispatchNotification.getDestination() + " does not exist for consumer "
                    + messageDispatchNotification.getConsumerId() + " with message: "
                    + messageDispatchNotification.getMessageId());
        }
    }

    public void gc() {
        for (Iterator<Subscription> iter = subscriptions.values().iterator(); iter.hasNext();) {
            Subscription sub = iter.next();
            sub.gc();
        }
        synchronized (destinationsMutex) {
            for (Iterator<Destination> iter = destinations.values().iterator(); iter.hasNext();) {
                Destination dest = iter.next();
                dest.gc();
            }
        }
    }

    protected abstract Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws Exception;

    protected Destination createDestination(ConnectionContext context, ActiveMQDestination destination)
            throws Exception {
        return destinationFactory.createDestination(context, destination, destinationStatistics);
    }

    public boolean isAutoCreateDestinations() {
        return autoCreateDestinations;
    }

    public void setAutoCreateDestinations(boolean autoCreateDestinations) {
        this.autoCreateDestinations = autoCreateDestinations;
    }

    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        synchronized (destinationsMutex) {
            for (Iterator iter = destinationMap.get(info.getDestination()).iterator(); iter.hasNext();) {
                Destination dest = (Destination) iter.next();
                dest.addProducer(context, info);
            }
        }
    }

    /**
     * Removes a Producer.
     * 
     * @param context
     *            the environment the operation is being executed under.
     * @throws Exception
     *             TODO
     */
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        synchronized (destinationsMutex) {
            for (Iterator iter = destinationMap.get(info.getDestination()).iterator(); iter.hasNext();) {
                Destination dest = (Destination) iter.next();
                dest.removeProducer(context, info);
            }
        }
    }

    protected void dispose(ConnectionContext context, Destination dest) throws Exception {
        dest.dispose(context);
        dest.stop();
        destinationFactory.removeDestination(dest);
    }

    public void processConsumerControl(ConsumerBrokerExchange consumerExchange, ConsumerControl control) {
        Subscription sub = subscriptions.get(control.getConsumerId());
        if (sub != null && sub instanceof AbstractSubscription) {
            ((AbstractSubscription) sub).setPrefetchSize(control.getPrefetch());
            if (LOG.isDebugEnabled()) {
                LOG.debug("setting prefetch: " + control.getPrefetch() + ", on subscription: "
                        + control.getConsumerId());
            }
            try {
                lookup(consumerExchange.getConnectionContext(), control.getDestination(),false).wakeup();
            } catch (Exception e) {
                LOG.warn("failed to deliver consumerControl to destination: " + control.getDestination(), e);
            }
        }
    }
}
