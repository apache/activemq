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
package org.apache.activemq.advisory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.filter.DestinationPath;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This broker filter handles tracking the state of the broker for purposes of
 * publishing advisory messages to advisory consumers.
 */
public class AdvisoryBroker extends BrokerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AdvisoryBroker.class);
    private static final IdGenerator ID_GENERATOR = new IdGenerator();

    protected final ConcurrentMap<ConnectionId, ConnectionInfo> connections = new ConcurrentHashMap<ConnectionId, ConnectionInfo>();

    private final ReentrantReadWriteLock consumersLock = new ReentrantReadWriteLock();
    protected final Map<ConsumerId, ConsumerInfo> consumers = new LinkedHashMap<ConsumerId, ConsumerInfo>();

    /**
     * This is a set to track all of the virtual destinations that have been added to the broker so
     * they can be easily referenced later.
     */
    protected final Set<VirtualDestination> virtualDestinations = Collections.newSetFromMap(new ConcurrentHashMap<VirtualDestination, Boolean>());
    /**
     * This is a map to track all consumers that exist on the virtual destination so that we can fire
     * an advisory later when they go away to remove the demand.
     */
    protected final ConcurrentMap<ConsumerInfo, VirtualDestination> virtualDestinationConsumers = new ConcurrentHashMap<>();
    /**
     * This is a map to track unique demand for the existence of a virtual destination so we make sure
     * we don't send duplicate advisories.
     */
    protected final ConcurrentMap<VirtualConsumerPair, ConsumerInfo> brokerConsumerDests = new ConcurrentHashMap<>();

    protected final ConcurrentMap<ProducerId, ProducerInfo> producers = new ConcurrentHashMap<ProducerId, ProducerInfo>();
    protected final ConcurrentMap<ActiveMQDestination, DestinationInfo> destinations = new ConcurrentHashMap<ActiveMQDestination, DestinationInfo>();
    protected final ConcurrentMap<BrokerInfo, ActiveMQMessage> networkBridges = new ConcurrentHashMap<BrokerInfo, ActiveMQMessage>();
    protected final ProducerId advisoryProducerId = new ProducerId();

    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();

    private VirtualDestinationMatcher virtualDestinationMatcher = new DestinationFilterVirtualDestinationMatcher();

    public AdvisoryBroker(Broker next) {
        super(next);
        advisoryProducerId.setConnectionId(ID_GENERATOR.generateId());
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        super.addConnection(context, info);

        ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
        // do not distribute passwords in advisory messages. usernames okay
        ConnectionInfo copy = info.copy();
        copy.setPassword("");
        fireAdvisory(context, topic, copy);
        connections.put(copy.getConnectionId(), copy);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription answer = super.addConsumer(context, info);

        // Don't advise advisory topics.
        if (!AdvisorySupport.isAdvisoryTopic(info.getDestination())) {
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(info.getDestination());
            consumersLock.writeLock().lock();
            try {
                consumers.put(info.getConsumerId(), info);

                //check if this is a consumer on a destination that matches a virtual destination
                if (getBrokerService().isUseVirtualDestSubs()) {
                    for (VirtualDestination virtualDestination : virtualDestinations) {
                        if (virtualDestinationMatcher.matches(virtualDestination, info.getDestination())) {
                            fireVirtualDestinationAddAdvisory(context, info, info.getDestination(), virtualDestination);
                        }
                    }
                }
            } finally {
                consumersLock.writeLock().unlock();
            }
            fireConsumerAdvisory(context, info.getDestination(), topic, info);
        } else {
            // We need to replay all the previously collected state objects
            // for this newly added consumer.
            if (AdvisorySupport.isConnectionAdvisoryTopic(info.getDestination())) {
                // Replay the connections.
                for (Iterator<ConnectionInfo> iter = connections.values().iterator(); iter.hasNext(); ) {
                    ConnectionInfo value = iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
                    fireAdvisory(context, topic, value, info.getConsumerId());
                }
            }

            // We check here whether the Destination is Temporary Destination specific or not since we
            // can avoid sending advisory messages to the consumer if it only wants Temporary Destination
            // notifications.  If its not just temporary destination related destinations then we have
            // to send them all, a composite destination could want both.
            if (AdvisorySupport.isTempDestinationAdvisoryTopic(info.getDestination())) {
                // Replay the temporary destinations.
                for (DestinationInfo destination : destinations.values()) {
                    if (destination.getDestination().isTemporary()) {
                        ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination.getDestination());
                        fireAdvisory(context, topic, destination, info.getConsumerId());
                    }
                }
            } else if (AdvisorySupport.isDestinationAdvisoryTopic(info.getDestination())) {
                // Replay all the destinations.
                for (DestinationInfo destination : destinations.values()) {
                    ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination.getDestination());
                    fireAdvisory(context, topic, destination, info.getConsumerId());
                }
            }

            // Replay the producers.
            if (AdvisorySupport.isProducerAdvisoryTopic(info.getDestination())) {
                for (Iterator<ProducerInfo> iter = producers.values().iterator(); iter.hasNext(); ) {
                    ProducerInfo value = iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(value.getDestination());
                    fireProducerAdvisory(context, value.getDestination(), topic, value, info.getConsumerId());
                }
            }

            // Replay the consumers.
            if (AdvisorySupport.isConsumerAdvisoryTopic(info.getDestination())) {
                consumersLock.readLock().lock();
                try {
                    for (Iterator<ConsumerInfo> iter = consumers.values().iterator(); iter.hasNext(); ) {
                        ConsumerInfo value = iter.next();
                        ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(value.getDestination());
                        fireConsumerAdvisory(context, value.getDestination(), topic, value, info.getConsumerId());
                    }
                } finally {
                    consumersLock.readLock().unlock();
                }
            }

            // Replay the virtual destination consumers.
            if (AdvisorySupport.isVirtualDestinationConsumerAdvisoryTopic(info.getDestination())) {
                for (Iterator<ConsumerInfo> iter = virtualDestinationConsumers.keySet().iterator(); iter.hasNext(); ) {
                    ConsumerInfo key = iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getVirtualDestinationConsumerAdvisoryTopic(key.getDestination());
                    fireConsumerAdvisory(context, key.getDestination(), topic, key);
              }
            }

            // Replay network bridges
            if (AdvisorySupport.isNetworkBridgeAdvisoryTopic(info.getDestination())) {
                for (Iterator<BrokerInfo> iter = networkBridges.keySet().iterator(); iter.hasNext(); ) {
                    BrokerInfo key = iter.next();
                    ActiveMQTopic topic = AdvisorySupport.getNetworkBridgeAdvisoryTopic();
                    fireAdvisory(context, topic, key, null, networkBridges.get(key));
                }
            }
        }
        return answer;
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.addProducer(context, info);

        // Don't advise advisory topics.
        if (info.getDestination() != null && !AdvisorySupport.isAdvisoryTopic(info.getDestination())) {
            ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(info.getDestination());
            fireProducerAdvisory(context, info.getDestination(), topic, info);
            producers.put(info.getProducerId(), info);
        }
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean create) throws Exception {
        Destination answer = super.addDestination(context, destination, create);
        if (!AdvisorySupport.isAdvisoryTopic(destination)) {
            //for queues, create demand if isUseVirtualDestSubsOnCreation is true
            if (getBrokerService().isUseVirtualDestSubsOnCreation() && destination.isQueue()) {
                //check if this new destination matches a virtual destination that exists
                for (VirtualDestination virtualDestination : virtualDestinations) {
                    if (virtualDestinationMatcher.matches(virtualDestination, destination)) {
                        fireVirtualDestinationAddAdvisory(context, null, destination, virtualDestination);
                    }
                }
            }

            DestinationInfo info = new DestinationInfo(context.getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, destination);
            DestinationInfo previous = destinations.putIfAbsent(destination, info);
            if (previous == null) {
                ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
                fireAdvisory(context, topic, info);
            }
        }
        return answer;
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        next.addDestinationInfo(context, info);

        if (!AdvisorySupport.isAdvisoryTopic(destination)) {
            DestinationInfo previous = destinations.putIfAbsent(destination, info);
            if (previous == null) {
                ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
                fireAdvisory(context, topic, info);
            }
        }
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        DestinationInfo info = destinations.remove(destination);
        if (info != null) {

            //on destination removal, remove all demand if using virtual dest subs
            if (getBrokerService().isUseVirtualDestSubs()) {
                for (ConsumerInfo consumerInfo : virtualDestinationConsumers.keySet()) {
                    //find all consumers for this virtual destination
                    VirtualDestination virtualDestination = virtualDestinationConsumers.get(consumerInfo);

                    //find a consumer that matches this virtualDest and destination
                    if (virtualDestinationMatcher.matches(virtualDestination, destination)) {
                        //in case of multiple matches
                        VirtualConsumerPair key = new VirtualConsumerPair(virtualDestination, destination);
                        ConsumerInfo i = brokerConsumerDests.get(key);
                        if (consumerInfo.equals(i) && brokerConsumerDests.remove(key) != null) {
                            LOG.debug("Virtual consumer pair removed: {} for consumer: {} ", key, i);
                            fireVirtualDestinationRemoveAdvisory(context, consumerInfo);
                            break;
                        }
                    }
                }
            }

            // ensure we don't modify (and loose/overwrite) an in-flight add advisory, so duplicate
            info = info.copy();
            info.setDestination(destination);
            info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
            ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
            fireAdvisory(context, topic, info);
            ActiveMQTopic[] advisoryDestinations = AdvisorySupport.getAllDestinationAdvisoryTopics(destination);
            for (ActiveMQTopic advisoryDestination : advisoryDestinations) {
                try {
                    next.removeDestination(context, advisoryDestination, -1);
                } catch (Exception expectedIfDestinationDidNotExistYet) {
                }
            }
        }
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo destInfo) throws Exception {
        super.removeDestinationInfo(context, destInfo);
        DestinationInfo info = destinations.remove(destInfo.getDestination());
        if (info != null) {
            // ensure we don't modify (and loose/overwrite) an in-flight add advisory, so duplicate
            info = info.copy();
            info.setDestination(destInfo.getDestination());
            info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
            ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(destInfo.getDestination());
            fireAdvisory(context, topic, info);
            ActiveMQTopic[] advisoryDestinations = AdvisorySupport.getAllDestinationAdvisoryTopics(destInfo.getDestination());
            for (ActiveMQTopic advisoryDestination : advisoryDestinations) {
                try {
                    next.removeDestination(context, advisoryDestination, -1);
                } catch (Exception expectedIfDestinationDidNotExistYet) {
                }
            }
        }
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        super.removeConnection(context, info, error);

        ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
        fireAdvisory(context, topic, info.createRemoveCommand());
        connections.remove(info.getConnectionId());
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        super.removeConsumer(context, info);

        // Don't advise advisory topics.
        ActiveMQDestination dest = info.getDestination();
        if (!AdvisorySupport.isAdvisoryTopic(dest)) {
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(dest);
            consumersLock.writeLock().lock();
            try {
                consumers.remove(info.getConsumerId());

                //remove the demand for this consumer if it matches a virtual destination
                if(getBrokerService().isUseVirtualDestSubs()) {
                    fireVirtualDestinationRemoveAdvisory(context, info);
                }
            } finally {
                consumersLock.writeLock().unlock();
            }
            if (!dest.isTemporary() || destinations.containsKey(dest)) {
                fireConsumerAdvisory(context, dest, topic, info.createRemoveCommand());
            }
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());

        RegionBroker regionBroker = null;
        if (next instanceof RegionBroker) {
            regionBroker = (RegionBroker) next;
        } else {
            BrokerService service = next.getBrokerService();
            regionBroker = (RegionBroker) service.getRegionBroker();
        }

        if (regionBroker == null) {
            LOG.warn("Cannot locate a RegionBroker instance to pass along the removeSubscription call");
            throw new IllegalStateException("No RegionBroker found.");
        }

        DurableTopicSubscription sub = ((TopicRegion) regionBroker.getTopicRegion()).getDurableSubscription(key);

        super.removeSubscription(context, info);

        if (sub == null) {
            LOG.warn("We cannot send an advisory message for a durable sub removal when we don't know about the durable sub");
            return;
        }

        ActiveMQDestination dest = sub.getConsumerInfo().getDestination();

        // Don't advise advisory topics.
        if (!AdvisorySupport.isAdvisoryTopic(dest)) {
            ActiveMQTopic topic = AdvisorySupport.getConsumerAdvisoryTopic(dest);
            fireConsumerAdvisory(context, dest, topic, info);
        }

    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.removeProducer(context, info);

        // Don't advise advisory topics.
        ActiveMQDestination dest = info.getDestination();
        if (info.getDestination() != null && !AdvisorySupport.isAdvisoryTopic(dest)) {
            ActiveMQTopic topic = AdvisorySupport.getProducerAdvisoryTopic(dest);
            producers.remove(info.getProducerId());
            if (!dest.isTemporary() || destinations.containsKey(dest)) {
                fireProducerAdvisory(context, dest, topic, info.createRemoveCommand());
            }
        }
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference messageReference, Subscription subscription) {
        super.messageExpired(context, messageReference, subscription);
        try {
            if (!messageReference.isAdvisory()) {
                BaseDestination baseDestination = (BaseDestination) messageReference.getMessage().getRegionDestination();
                ActiveMQTopic topic = AdvisorySupport.getExpiredMessageTopic(baseDestination.getActiveMQDestination());
                Message payload = messageReference.getMessage().copy();
                if (!baseDestination.isIncludeBodyForAdvisory()) {
                    payload.clearBody();
                }
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_MESSAGE_ID, payload.getMessageId().toString());
                fireAdvisory(context, topic, payload, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("expired", e);
        }
    }

    @Override
    public void messageConsumed(ConnectionContext context, MessageReference messageReference) {
        super.messageConsumed(context, messageReference);
        try {
            if (!messageReference.isAdvisory()) {
                BaseDestination baseDestination = (BaseDestination) messageReference.getMessage().getRegionDestination();
                ActiveMQTopic topic = AdvisorySupport.getMessageConsumedAdvisoryTopic(baseDestination.getActiveMQDestination());
                Message payload = messageReference.getMessage().copy();
                if (!baseDestination.isIncludeBodyForAdvisory()) {
                    payload.clearBody();
                }
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_MESSAGE_ID, payload.getMessageId().toString());
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION, baseDestination.getActiveMQDestination().getQualifiedName());
                fireAdvisory(context, topic, payload, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("consumed", e);
        }
    }

    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        super.messageDelivered(context, messageReference);
        try {
            if (!messageReference.isAdvisory()) {
                BaseDestination baseDestination = (BaseDestination) messageReference.getMessage().getRegionDestination();
                ActiveMQTopic topic = AdvisorySupport.getMessageDeliveredAdvisoryTopic(baseDestination.getActiveMQDestination());
                Message payload = messageReference.getMessage().copy();
                if (!baseDestination.isIncludeBodyForAdvisory()) {
                    payload.clearBody();
                }
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_MESSAGE_ID, payload.getMessageId().toString());
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION, baseDestination.getActiveMQDestination().getQualifiedName());
                fireAdvisory(context, topic, payload, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("delivered", e);
        }
    }

    @Override
    public void messageDiscarded(ConnectionContext context, Subscription sub, MessageReference messageReference) {
        super.messageDiscarded(context, sub, messageReference);
        try {
            if (!messageReference.isAdvisory()) {
                BaseDestination baseDestination = (BaseDestination) messageReference.getMessage().getRegionDestination();
                ActiveMQTopic topic = AdvisorySupport.getMessageDiscardedAdvisoryTopic(baseDestination.getActiveMQDestination());
                Message payload = messageReference.getMessage().copy();
                if (!baseDestination.isIncludeBodyForAdvisory()) {
                    payload.clearBody();
                }
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                if (sub instanceof TopicSubscription) {
                    advisoryMessage.setIntProperty(AdvisorySupport.MSG_PROPERTY_DISCARDED_COUNT, ((TopicSubscription) sub).discarded());
                }
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_MESSAGE_ID, payload.getMessageId().toString());
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID, sub.getConsumerInfo().getConsumerId().toString());
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION, baseDestination.getActiveMQDestination().getQualifiedName());

                fireAdvisory(context, topic, payload, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("discarded", e);
        }
    }

    @Override
    public void slowConsumer(ConnectionContext context, Destination destination, Subscription subs) {
        super.slowConsumer(context, destination, subs);
        try {
            if (!AdvisorySupport.isAdvisoryTopic(destination.getActiveMQDestination())) {
                ActiveMQTopic topic = AdvisorySupport.getSlowConsumerAdvisoryTopic(destination.getActiveMQDestination());
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID, subs.getConsumerInfo().getConsumerId().toString());
                fireAdvisory(context, topic, subs.getConsumerInfo(), null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("slow consumer", e);
        }
    }

    @Override
    public void fastProducer(ConnectionContext context, ProducerInfo producerInfo, ActiveMQDestination destination) {
        super.fastProducer(context, producerInfo, destination);
        try {
            if (!AdvisorySupport.isAdvisoryTopic(destination)) {
                ActiveMQTopic topic = AdvisorySupport.getFastProducerAdvisoryTopic(destination);
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_PRODUCER_ID, producerInfo.getProducerId().toString());
                fireAdvisory(context, topic, producerInfo, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("fast producer", e);
        }
    }

    private final IdGenerator connectionIdGenerator = new IdGenerator("advisory");
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();

    @Override
    public void virtualDestinationAdded(ConnectionContext context,
            VirtualDestination virtualDestination) {
        super.virtualDestinationAdded(context, virtualDestination);

        if (virtualDestinations.add(virtualDestination)) {
            LOG.debug("Virtual destination added: {}", virtualDestination);
            try {
                // Don't advise advisory topics.
                if (!AdvisorySupport.isAdvisoryTopic(virtualDestination.getVirtualDestination())) {

                    //create demand for consumers on virtual destinations
                    consumersLock.readLock().lock();
                    try {
                        //loop through existing destinations to see if any match this newly
                        //created virtual destination
                        if (getBrokerService().isUseVirtualDestSubsOnCreation()) {
                            //for matches that are a queue, fire an advisory for demand
                            for (ActiveMQDestination destination : destinations.keySet()) {
                                if(destination.isQueue()) {
                                    if (virtualDestinationMatcher.matches(virtualDestination, destination)) {
                                        fireVirtualDestinationAddAdvisory(context, null, destination, virtualDestination);
                                    }
                                }
                            }
                        }

                        //loop through existing consumers to see if any of them are consuming on a destination
                        //that matches the new virtual destination
                        for (Iterator<ConsumerInfo> iter = consumers.values().iterator(); iter.hasNext(); ) {
                            ConsumerInfo info = iter.next();
                            if (virtualDestinationMatcher.matches(virtualDestination, info.getDestination())) {
                                fireVirtualDestinationAddAdvisory(context, info, info.getDestination(), virtualDestination);
                            }
                        }
                    } finally {
                        consumersLock.readLock().unlock();
                    }
                }
            } catch (Exception e) {
                handleFireFailure("virtualDestinationAdded", e);
            }
        }
    }

    private void fireVirtualDestinationAddAdvisory(ConnectionContext context, ConsumerInfo info, ActiveMQDestination activeMQDest,
            VirtualDestination virtualDestination) throws Exception {
        //if no consumer info, we need to create one - this is the case when an advisory is fired
        //because of the existence of a destination matching a virtual destination
        if (info == null) {

            //store the virtual destination and the activeMQDestination as a pair so that we can keep track
            //of all matching forwarded destinations that caused demand
            VirtualConsumerPair pair = new VirtualConsumerPair(virtualDestination, activeMQDest);
            if (brokerConsumerDests.get(pair) == null) {
                ConnectionId connectionId = new ConnectionId(connectionIdGenerator.generateId());
                SessionId sessionId = new SessionId(connectionId, sessionIdGenerator.getNextSequenceId());
                ConsumerId consumerId = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
                info = new ConsumerInfo(consumerId);

                if(brokerConsumerDests.putIfAbsent(pair, info) == null) {
                    LOG.debug("Virtual consumer pair added: {} for consumer: {} ", pair, info);
                    setConsumerInfoVirtualDest(info, virtualDestination, activeMQDest);
                    ActiveMQTopic topic = AdvisorySupport.getVirtualDestinationConsumerAdvisoryTopic(info.getDestination());

                    if (virtualDestinationConsumers.putIfAbsent(info, virtualDestination) == null) {
                        LOG.debug("Virtual consumer added: {}, for virtual destination: {}", info, virtualDestination);
                        fireConsumerAdvisory(context, info.getDestination(), topic, info);
                    }
                }
            }
        //this is the case of a real consumer coming online
        } else {
            info = info.copy();
            setConsumerInfoVirtualDest(info, virtualDestination, activeMQDest);
            ActiveMQTopic topic = AdvisorySupport.getVirtualDestinationConsumerAdvisoryTopic(info.getDestination());

            if (virtualDestinationConsumers.putIfAbsent(info, virtualDestination) == null) {
                LOG.debug("Virtual consumer added: {}, for virtual destination: {}", info, virtualDestination);
                fireConsumerAdvisory(context, info.getDestination(), topic, info);
            }
        }
    }

    /**
     * Sets the virtual destination on the ConsumerInfo
     * If this is a VirtualTopic then the destination used will be the actual topic subscribed
     * to in order to track demand properly
     *
     * @param info
     * @param virtualDestination
     * @param activeMQDest
     */
    private void setConsumerInfoVirtualDest(ConsumerInfo info, VirtualDestination virtualDestination, ActiveMQDestination activeMQDest) {
        info.setDestination(virtualDestination.getVirtualDestination());
        if (virtualDestination instanceof VirtualTopic) {
            VirtualTopic vt = (VirtualTopic) virtualDestination;
            String prefix = vt.getPrefix() != null ? vt.getPrefix() : "";
            String postfix = vt.getPostfix() != null ? vt.getPostfix() : "";
            if (prefix.endsWith(".")) {
                prefix = prefix.substring(0, prefix.length() - 1);
            }
            if (postfix.startsWith(".")) {
                postfix = postfix.substring(1, postfix.length());
            }
            ActiveMQDestination prefixDestination = prefix.length() > 0 ? new ActiveMQTopic(prefix) : null;
            ActiveMQDestination postfixDestination = postfix.length() > 0 ? new ActiveMQTopic(postfix) : null;

            String[] prefixPaths = prefixDestination != null ? prefixDestination.getDestinationPaths() : new String[] {};
            String[] activeMQDestPaths = activeMQDest.getDestinationPaths();
            String[] postfixPaths = postfixDestination != null ? postfixDestination.getDestinationPaths() : new String[] {};

            //sanity check
            if (activeMQDestPaths.length > prefixPaths.length + postfixPaths.length) {
                String[] topicPath = Arrays.copyOfRange(activeMQDestPaths, 0 + prefixPaths.length,
                        activeMQDestPaths.length - postfixPaths.length);

                ActiveMQTopic newTopic = new ActiveMQTopic(DestinationPath.toString(topicPath));
                info.setDestination(newTopic);
            }
        }
    }

    @Override
    public void virtualDestinationRemoved(ConnectionContext context,
            VirtualDestination virtualDestination) {
        super.virtualDestinationRemoved(context, virtualDestination);

        if (virtualDestinations.remove(virtualDestination)) {
            LOG.debug("Virtual destination removed: {}", virtualDestination);
            try {
                consumersLock.readLock().lock();
                try {
                    // remove the demand created by the addition of the virtual destination
                    if (getBrokerService().isUseVirtualDestSubsOnCreation()) {
                        if (!AdvisorySupport.isAdvisoryTopic(virtualDestination.getVirtualDestination())) {
                            for (ConsumerInfo info : virtualDestinationConsumers.keySet()) {
                                //find all consumers for this virtual destination
                                if (virtualDestinationConsumers.get(info).equals(virtualDestination)) {
                                    fireVirtualDestinationRemoveAdvisory(context, info);

                                    //check consumers created for the existence of a destination to see if they
                                    //match the consumerinfo and clean up
                                    for (VirtualConsumerPair activeMQDest : brokerConsumerDests.keySet()) {
                                        ConsumerInfo i = brokerConsumerDests.get(activeMQDest);
                                        if (info.equals(i) && brokerConsumerDests.remove(activeMQDest) != null) {
                                            LOG.debug("Virtual consumer pair removed: {} for consumer: {} ", activeMQDest, i);
                                        }
                                    }
                                }

                            }
                        }
                    }
                } finally {
                    consumersLock.readLock().unlock();
                }
            } catch (Exception e) {
                handleFireFailure("virtualDestinationAdded", e);
            }
        }
    }

    private void fireVirtualDestinationRemoveAdvisory(ConnectionContext context,
            ConsumerInfo info) throws Exception {

        VirtualDestination virtualDestination = virtualDestinationConsumers.remove(info);
        if (virtualDestination != null) {
            LOG.debug("Virtual consumer removed: {}, for virtual destination: {}", info, virtualDestination);
            ActiveMQTopic topic = AdvisorySupport.getVirtualDestinationConsumerAdvisoryTopic(virtualDestination.getVirtualDestination());

            ActiveMQDestination dest = info.getDestination();

            if (!dest.isTemporary() || destinations.containsKey(dest)) {
                fireConsumerAdvisory(context, dest, topic, info.createRemoveCommand());
            }
        }
    }

    @Override
    public void isFull(ConnectionContext context, Destination destination, Usage<?> usage) {
        super.isFull(context, destination, usage);
        if (AdvisorySupport.isAdvisoryTopic(destination.getActiveMQDestination()) == false) {
            try {

                ActiveMQTopic topic = AdvisorySupport.getFullAdvisoryTopic(destination.getActiveMQDestination());
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_USAGE_NAME, usage.getName());
                advisoryMessage.setLongProperty(AdvisorySupport.MSG_PROPERTY_USAGE_COUNT, usage.getUsage());
                fireAdvisory(context, topic, null, null, advisoryMessage);

            } catch (Exception e) {
                handleFireFailure("is full", e);
            }
        }
    }

    @Override
    public void nowMasterBroker() {
        super.nowMasterBroker();
        try {
            ActiveMQTopic topic = AdvisorySupport.getMasterBrokerAdvisoryTopic();
            ActiveMQMessage advisoryMessage = new ActiveMQMessage();
            ConnectionContext context = new ConnectionContext();
            context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
            context.setBroker(getBrokerService().getBroker());
            fireAdvisory(context, topic, null, null, advisoryMessage);
        } catch (Exception e) {
            handleFireFailure("now master broker", e);
        }
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference,
                                         Subscription subscription, Throwable poisonCause) {
        boolean wasDLQd = super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
        if (wasDLQd) {
            try {
                if (!messageReference.isAdvisory()) {
                    BaseDestination baseDestination = (BaseDestination) messageReference.getMessage().getRegionDestination();
                    ActiveMQTopic topic = AdvisorySupport.getMessageDLQdAdvisoryTopic(baseDestination.getActiveMQDestination());
                    Message payload = messageReference.getMessage().copy();
                    if (!baseDestination.isIncludeBodyForAdvisory()) {
                        payload.clearBody();
                    }
                    fireAdvisory(context, topic, payload);
                }
            } catch (Exception e) {
                handleFireFailure("add to DLQ", e);
            }
        }

        return wasDLQd;
    }

    @Override
    public void networkBridgeStarted(BrokerInfo brokerInfo, boolean createdByDuplex, String remoteIp) {
        try {
            if (brokerInfo != null) {
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setBooleanProperty("started", true);
                advisoryMessage.setBooleanProperty("createdByDuplex", createdByDuplex);
                advisoryMessage.setStringProperty("remoteIp", remoteIp);
                networkBridges.putIfAbsent(brokerInfo, advisoryMessage);

                ActiveMQTopic topic = AdvisorySupport.getNetworkBridgeAdvisoryTopic();

                ConnectionContext context = new ConnectionContext();
                context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
                context.setBroker(getBrokerService().getBroker());
                fireAdvisory(context, topic, brokerInfo, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("network bridge started", e);
        }
    }

    @Override
    public void networkBridgeStopped(BrokerInfo brokerInfo) {
        try {
            if (brokerInfo != null) {
                ActiveMQMessage advisoryMessage = new ActiveMQMessage();
                advisoryMessage.setBooleanProperty("started", false);
                networkBridges.remove(brokerInfo);

                ActiveMQTopic topic = AdvisorySupport.getNetworkBridgeAdvisoryTopic();

                ConnectionContext context = new ConnectionContext();
                context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
                context.setBroker(getBrokerService().getBroker());
                fireAdvisory(context, topic, brokerInfo, null, advisoryMessage);
            }
        } catch (Exception e) {
            handleFireFailure("network bridge stopped", e);
        }
    }

    private void handleFireFailure(String message, Throwable cause) {
        LOG.warn("Failed to fire {} advisory, reason: {}", message, cause);
        LOG.debug("{} detail: {}", message, cause, cause);
    }

    protected void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command) throws Exception {
        fireAdvisory(context, topic, command, null);
    }

    protected void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Exception {
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
        fireAdvisory(context, topic, command, targetConsumerId, advisoryMessage);
    }

    protected void fireConsumerAdvisory(ConnectionContext context, ActiveMQDestination consumerDestination, ActiveMQTopic topic, Command command) throws Exception {
        fireConsumerAdvisory(context, consumerDestination, topic, command, null);
    }

    protected void fireConsumerAdvisory(ConnectionContext context, ActiveMQDestination consumerDestination, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Exception {
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
        int count = 0;
        Set<Destination> set = getDestinations(consumerDestination);
        if (set != null) {
            for (Destination dest : set) {
                count += dest.getDestinationStatistics().getConsumers().getCount();
            }
        }
        advisoryMessage.setIntProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_COUNT, count);

        fireAdvisory(context, topic, command, targetConsumerId, advisoryMessage);
    }

    protected void fireProducerAdvisory(ConnectionContext context, ActiveMQDestination producerDestination, ActiveMQTopic topic, Command command) throws Exception {
        fireProducerAdvisory(context, producerDestination, topic, command, null);
    }

    protected void fireProducerAdvisory(ConnectionContext context, ActiveMQDestination producerDestination, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId) throws Exception {
        ActiveMQMessage advisoryMessage = new ActiveMQMessage();
        int count = 0;
        if (producerDestination != null) {
            Set<Destination> set = getDestinations(producerDestination);
            if (set != null) {
                for (Destination dest : set) {
                    count += dest.getDestinationStatistics().getProducers().getCount();
                }
            }
        }
        advisoryMessage.setIntProperty("producerCount", count);
        fireAdvisory(context, topic, command, targetConsumerId, advisoryMessage);
    }

    public void fireAdvisory(ConnectionContext context, ActiveMQTopic topic, Command command, ConsumerId targetConsumerId, ActiveMQMessage advisoryMessage) throws Exception {
        //set properties
        advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_NAME, getBrokerName());
        String id = getBrokerId() != null ? getBrokerId().getValue() : "NOT_SET";
        advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, id);

        String url = getBrokerService().getVmConnectorURI().toString();
        //try and find the URL on the transport connector and use if it exists else
        //try and find a default URL
        if (context.getConnector() instanceof TransportConnector
                && ((TransportConnector) context.getConnector()).getPublishableConnectString() != null) {
            url = ((TransportConnector) context.getConnector()).getPublishableConnectString();
        } else if (getBrokerService().getDefaultSocketURIString() != null) {
            url = getBrokerService().getDefaultSocketURIString();
        }
        advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL, url);

        //set the data structure
        advisoryMessage.setDataStructure(command);
        advisoryMessage.setPersistent(false);
        advisoryMessage.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
        advisoryMessage.setMessageId(new MessageId(advisoryProducerId, messageIdGenerator.getNextSequenceId()));
        advisoryMessage.setTargetConsumerId(targetConsumerId);
        advisoryMessage.setDestination(topic);
        advisoryMessage.setResponseRequired(false);
        advisoryMessage.setProducerId(advisoryProducerId);
        boolean originalFlowControl = context.isProducerFlowControl();
        final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(context);
        producerExchange.setMutable(true);
        producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
        try {
            context.setProducerFlowControl(false);
            next.send(producerExchange, advisoryMessage);
        } finally {
            context.setProducerFlowControl(originalFlowControl);
        }
    }

    public Map<ConnectionId, ConnectionInfo> getAdvisoryConnections() {
        return connections;
    }

    public Collection<ConsumerInfo> getAdvisoryConsumers() {
        consumersLock.readLock().lock();
        try {
            return new ArrayList<ConsumerInfo>(consumers.values());
        } finally {
            consumersLock.readLock().unlock();
        }
    }

    public Map<ProducerId, ProducerInfo> getAdvisoryProducers() {
        return producers;
    }

    public Map<ActiveMQDestination, DestinationInfo> getAdvisoryDestinations() {
        return destinations;
    }

    public ConcurrentMap<ConsumerInfo, VirtualDestination> getVirtualDestinationConsumers() {
        return virtualDestinationConsumers;
    }

    private class VirtualConsumerPair {
        private final VirtualDestination virtualDestination;

        //destination that matches this virtualDestination as part target
        //this is so we can keep track of more than one destination that might
        //match the virtualDestination and cause demand
        private final ActiveMQDestination activeMQDestination;

        public VirtualConsumerPair(VirtualDestination virtualDestination,
                ActiveMQDestination activeMQDestination) {
            super();
            this.virtualDestination = virtualDestination;
            this.activeMQDestination = activeMQDestination;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime
                    * result
                    + ((activeMQDestination == null) ? 0 : activeMQDestination
                            .hashCode());
            result = prime
                    * result
                    + ((virtualDestination == null) ? 0 : virtualDestination
                            .hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            VirtualConsumerPair other = (VirtualConsumerPair) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (activeMQDestination == null) {
                if (other.activeMQDestination != null)
                    return false;
            } else if (!activeMQDestination.equals(other.activeMQDestination))
                return false;
            if (virtualDestination == null) {
                if (other.virtualDestination != null)
                    return false;
            } else if (!virtualDestination.equals(other.virtualDestination))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "VirtualConsumerPair [virtualDestination=" + virtualDestination + ", activeMQDestination="
                    + activeMQDestination + "]";
        }

        private AdvisoryBroker getOuterType() {
            return AdvisoryBroker.this;
        }
    }
}
