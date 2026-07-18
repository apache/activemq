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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQErrorCode;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SharedConsumerInfo;
import org.apache.activemq.command.SharedSubscriptionInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.SharedSubscriptionKey;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends {@link TopicRegion} to route {@link SharedConsumerInfo} commands
 * to shared subscription instances. Multiple consumers with the same
 * subscription name join a single subscription rather than creating
 * separate ones.
 *
 * <p>Persists the shared flag via {@link SharedSubscriptionInfo} so
 * shared durable subscriptions are correctly restored after broker restart.
 *
 * <p>Enforces JMS 3.1 type-conflict rules: shared and unshared durable
 * subscriptions may not have the same name and client identifier. Set
 * {@link #setTopicSubscriptionConversionEnabled(boolean)} to {@code true}
 * to allow automatic promotion/demotion instead of throwing.
 */
public class SharedTopicRegion extends TopicRegion {

    private static final Logger LOG = LoggerFactory.getLogger(SharedTopicRegion.class);

    private final ConcurrentHashMap<SharedSubscriptionKey, SharedDurableTopicSubscription> sharedDurableSubs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SharedSubscriptionKey, SharedTopicSubscription> sharedNonDurableSubs =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ConsumerId, SharedSubscriptionKey> consumerToSharedKey =
            new ConcurrentHashMap<>();

    private boolean topicSubscriptionConversionEnabled;

    public SharedTopicRegion(RegionBroker broker, DestinationStatistics destinationStatistics,
            SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory,
            DestinationFactory destinationFactory) {
        super(broker, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    public boolean isTopicSubscriptionConversionEnabled() {
        return topicSubscriptionConversionEnabled;
    }

    public void setTopicSubscriptionConversionEnabled(boolean topicSubscriptionConversionEnabled) {
        this.topicSubscriptionConversionEnabled = topicSubscriptionConversionEnabled;
    }

    // [Restore path]

    @Override
    public ConsumerInfo createInactiveConsumerInfo(SubscriptionInfo info) {
        ConsumerInfo base = super.createInactiveConsumerInfo(info);
        if (info instanceof SharedSubscriptionInfo && ((SharedSubscriptionInfo) info).isShared()) {
            SharedConsumerInfo shared = new SharedConsumerInfo(base.getConsumerId());
            shared.setSelector(base.getSelector());
            shared.setSubscriptionName(base.getSubscriptionName());
            shared.setDestination(base.getDestination());
            shared.setNoLocal(base.isNoLocal());
            shared.setShared(true);
            shared.setDurable(true);
            return shared;
        }
        return base;
    }

    @Override
    protected List<Subscription> addSubscriptionsForDestination(ConnectionContext context,
            Destination dest) throws Exception {
        List<Subscription> result = super.addSubscriptionsForDestination(context, dest);

        for (Subscription sub : result) {
            if (sub instanceof SharedDurableTopicSubscription) {
                SharedDurableTopicSubscription sharedSub = (SharedDurableTopicSubscription) sub;
                String clientId = sharedSub.getSubscriptionKey().getClientId();
                String subName = sharedSub.getSubscriptionKey().getSubscriptionName();
                SharedSubscriptionKey sharedKey = new SharedSubscriptionKey(clientId, subName);
                sharedDurableSubs.putIfAbsent(sharedKey, sharedSub);
                LOG.debug("Restored shared durable subscription '{}'", sharedKey);
            }
        }

        return result;
    }

    // [Consumer routing]

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (!(info instanceof SharedConsumerInfo) || !((SharedConsumerInfo) info).isShared()
                || !info.getNetworkConsumerIds().isEmpty()) {
            if (info.isDurable()) {
                checkUnsharedToSharedConflict(context, info);
            }
            return super.addConsumer(context, info);
        }

        SharedConsumerInfo sharedInfo = (SharedConsumerInfo) info;
        String effectiveClientId = effectiveClientId(context, sharedInfo);
        SharedSubscriptionKey sharedKey = new SharedSubscriptionKey(
                effectiveClientId, info.getSubscriptionName());

        if (sharedInfo.isDurable()) {
            return addSharedDurableConsumer(context, sharedInfo, sharedKey);
        } else {
            return addSharedNonDurableConsumer(context, sharedInfo, sharedKey);
        }
    }

    private Subscription addSharedDurableConsumer(ConnectionContext context,
            SharedConsumerInfo info, SharedSubscriptionKey sharedKey) throws Exception {
        normalizeClientId(context);

        SharedDurableTopicSubscription existing = sharedDurableSubs.get(sharedKey);
        if (existing != null) {
            validateSelectorMatch(info, existing.getConsumerInfo());
            existing.addConsumer(context, info);
            subscriptions.put(info.getConsumerId(), existing);
            consumerToSharedKey.put(info.getConsumerId(), sharedKey);
            if (!existing.isActive()) {
                existing.activate(usageManager, context, info, (RegionBroker) broker);
                onSharedDurableReactivated(context, existing);
            }
            LOG.debug("Consumer {} joined shared durable subscription '{}'",
                    info.getConsumerId(), sharedKey);
            return existing;
        }

        checkSharedToUnsharedConflict(context, info);

        LOG.debug("Consumer {} creating new shared durable subscription '{}'",
                info.getConsumerId(), sharedKey);
        Subscription sub = super.addConsumer(context, info);
        consumerToSharedKey.put(info.getConsumerId(), sharedKey);

        persistSharedFlag(context, info);

        return sub;
    }

    private Subscription addSharedNonDurableConsumer(ConnectionContext context,
            SharedConsumerInfo info, SharedSubscriptionKey sharedKey) throws Exception {
        normalizeClientId(context);

        SharedTopicSubscription existing = sharedNonDurableSubs.get(sharedKey);
        if (existing != null) {
            validateSelectorMatch(info, existing.getConsumerInfo());
            existing.addConsumer(context, info);
            subscriptions.put(info.getConsumerId(), existing);
            consumerToSharedKey.put(info.getConsumerId(), sharedKey);
            LOG.debug("Consumer {} joined shared non-durable subscription '{}'",
                    info.getConsumerId(), sharedKey);
            return existing;
        }

        Subscription sub = super.addConsumer(context, info);
        consumerToSharedKey.put(info.getConsumerId(), sharedKey);
        return sub;
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        SharedSubscriptionKey sharedKey = consumerToSharedKey.remove(info.getConsumerId());
        if (sharedKey == null) {
            super.removeConsumer(context, info);
            return;
        }

        if (info.isDurable()) {
            removeSharedDurableConsumer(context, info, sharedKey);
        } else {
            removeSharedNonDurableConsumer(context, info, sharedKey);
        }
    }

    private void removeSharedDurableConsumer(ConnectionContext context,
            ConsumerInfo info, SharedSubscriptionKey sharedKey) throws Exception {

        SharedDurableTopicSubscription sub = sharedDurableSubs.get(sharedKey);
        if (sub == null) {
            super.removeConsumer(context, info);
            return;
        }

        subscriptions.remove(info.getConsumerId());
        sub.removeConsumer(info.getConsumerId());
        LOG.debug("Consumer {} left shared durable subscription '{}', {} remaining",
                info.getConsumerId(), sharedKey, sub.getConsumerCount());

        if (!sub.hasConsumers()) {
            sub.deactivate(isKeepDurableSubsActive(), info.getLastDeliveredSequenceId());
        }
    }

    private void removeSharedNonDurableConsumer(ConnectionContext context,
            ConsumerInfo info, SharedSubscriptionKey sharedKey) throws Exception {

        SharedTopicSubscription sub = sharedNonDurableSubs.get(sharedKey);
        if (sub == null) {
            super.removeConsumer(context, info);
            return;
        }

        subscriptions.remove(info.getConsumerId());
        sub.removeConsumer(info.getConsumerId());

        if (!sub.hasConsumers()) {
            sharedNonDurableSubs.remove(sharedKey);
            removeSubscriptionFromDestinations(context, sub, info);
            onSharedNonDurableDestroyed(sub);
            sub.destroy();
        }
    }

    // [Unsubscribe]

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        SharedSubscriptionKey sharedKey = new SharedSubscriptionKey("", info.getSubscriptionName());
        SharedDurableTopicSubscription sharedSub = sharedDurableSubs.get(sharedKey);

        if (sharedSub == null && info.getClientId() != null) {
            sharedKey = new SharedSubscriptionKey(info.getClientId(), info.getSubscriptionName());
            sharedSub = sharedDurableSubs.get(sharedKey);
        }

        if (sharedSub == null) {
            super.removeSubscription(context, info);
            return;
        }

        if (sharedSub.isActive()) {
            throw new JMSException("Shared durable consumer is in use",
                    ActiveMQErrorCode.SUBSCRIPTION_IN_USE);
        }

        sharedDurableSubs.remove(sharedKey);

        SubscriptionKey durKey = null;
        for (Map.Entry<SubscriptionKey, DurableTopicSubscription> entry : durableSubscriptions.entrySet()) {
            if (entry.getValue() == sharedSub) {
                durKey = entry.getKey();
                break;
            }
        }
        if (durKey != null) {
            durableSubscriptions.remove(durKey);
            destinationsLock.readLock().lock();
            try {
                @SuppressWarnings("unchecked")
                Set<Destination> dests = destinationMap.unsynchronizedGet(
                        sharedSub.getConsumerInfo().getDestination());
                if (dests != null) {
                    for (Destination dest : dests) {
                        if (dest instanceof Topic) {
                            ((Topic) dest).deleteSubscription(context, durKey);
                        }
                    }
                }
            } finally {
                destinationsLock.readLock().unlock();
            }
        }

        destroySubscription(sharedSub);
    }

    // [Subscription factory]

    @Override
    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info)
            throws JMSException {
        if (!(info instanceof SharedConsumerInfo) || !((SharedConsumerInfo) info).isShared()) {
            return super.createSubscription(context, info);
        }

        SharedConsumerInfo sharedInfo = (SharedConsumerInfo) info;
        ActiveMQDestination destination = info.getDestination();

        if (sharedInfo.isDurable()) {
            return createSharedDurableSubscription(context, sharedInfo, destination);
        } else {
            return createSharedNonDurableSubscription(context, sharedInfo, destination);
        }
    }

    private Subscription createSharedDurableSubscription(ConnectionContext context,
            SharedConsumerInfo info, ActiveMQDestination destination) throws JMSException {

        String ecid = effectiveClientId(context, info);
        SharedSubscriptionKey sharedKey = new SharedSubscriptionKey(
                ecid, info.getSubscriptionName());

        String contextClientId = context.getClientId() != null ? context.getClientId() : "";
        SubscriptionKey subsKey = new SubscriptionKey(
                contextClientId, info.getSubscriptionName());

        if (durableSubscriptions.containsKey(subsKey)) {
            throw new JMSException("Shared durable subscription is already active: " +
                    info.getSubscriptionName(), ActiveMQErrorCode.SUBSCRIPTION_ALREADY_EXISTS);
        }

        SharedDurableTopicSubscription sub = new SharedDurableTopicSubscription(
                broker, usageManager, context, info, isKeepDurableSubsActive());

        applyPolicy(destination, sub);
        durableSubscriptions.put(subsKey, sub);
        sharedDurableSubs.put(sharedKey, sub);
        return sub;
    }

    private Subscription createSharedNonDurableSubscription(ConnectionContext context,
            SharedConsumerInfo info, ActiveMQDestination destination) throws JMSException {
        try {
            String ecid = effectiveClientId(context, info);
            SharedSubscriptionKey sharedKey = new SharedSubscriptionKey(
                    ecid, info.getSubscriptionName());

            SharedTopicSubscription sub = new SharedTopicSubscription(
                    broker, usageManager, context, info);

            applyPolicy(destination, sub);
            sharedNonDurableSubs.put(sharedKey, sub);
            return sub;
        } catch (Exception e) {
            JMSException jmsEx = new JMSException("Couldn't create shared TopicSubscription");
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }

    // [Store path]: persist shared flag

    private void persistSharedFlag(ConnectionContext context, SharedConsumerInfo info) {
        ActiveMQDestination destination = info.getDestination();
        if (destination == null || destination.isPattern()) {
            return;
        }
        destinationsLock.readLock().lock();
        try {
            @SuppressWarnings("unchecked")
            Set<Destination> dests = destinationMap.unsynchronizedGet(destination);
            if (dests != null) {
                for (Destination dest : dests) {
                    if (dest instanceof Topic) {
                        TopicMessageStore store = (TopicMessageStore) dest.getMessageStore();
                        if (store != null) {
                            SharedSubscriptionInfo sinfo = new SharedSubscriptionInfo();
                            sinfo.setClientId(context.getClientId());
                            sinfo.setSubscriptionName(info.getSubscriptionName());
                            sinfo.setSelector(info.getSelector());
                            sinfo.setDestination(dest.getActiveMQDestination());
                            sinfo.setSubscribedDestination(destination);
                            sinfo.setNoLocal(info.isNoLocal());
                            sinfo.setShared(true);
                            store.addSubscription(sinfo, info.isRetroactive());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to persist shared flag for subscription '{}': {}",
                    info.getSubscriptionName(), e.getMessage(), e);
        } finally {
            destinationsLock.readLock().unlock();
        }
    }

    // [Type-conflict guards]

    private void checkSharedToUnsharedConflict(ConnectionContext context,
            SharedConsumerInfo info) throws JMSException {
        SubscriptionKey subsKey = new SubscriptionKey(
                context.getClientId(), info.getSubscriptionName());
        DurableTopicSubscription existing = durableSubscriptions.get(subsKey);
        if (existing != null && !(existing instanceof SharedDurableTopicSubscription)) {
            if (!topicSubscriptionConversionEnabled) {
                throw new JMSException(
                        "A shared durable subscription and an unshared durable subscription " +
                        "may not have the same name and client identifier. " +
                        "Subscription '" + info.getSubscriptionName() +
                        "' exists as an unshared durable subscription.",
                        ActiveMQErrorCode.SUBSCRIPTION_TYPE_CONFLICT);
            }
            LOG.warn("Converting unshared durable subscription '{}' to shared " +
                    "(topicSubscriptionConversionEnabled=true)", info.getSubscriptionName());
            durableSubscriptions.remove(subsKey);
        }
    }

    private void checkUnsharedToSharedConflict(ConnectionContext context,
            ConsumerInfo info) throws JMSException {
        normalizeClientId(context);
        SubscriptionKey subsKey = new SubscriptionKey(
                context.getClientId(), info.getSubscriptionName());
        DurableTopicSubscription existing = durableSubscriptions.get(subsKey);
        if (existing instanceof SharedDurableTopicSubscription) {
            if (!topicSubscriptionConversionEnabled) {
                throw new JMSException(
                        "A shared durable subscription and an unshared durable subscription " +
                        "may not have the same name and client identifier. " +
                        "Subscription '" + info.getSubscriptionName() +
                        "' exists as a shared durable subscription.",
                        ActiveMQErrorCode.SUBSCRIPTION_TYPE_CONFLICT);
            }
            LOG.warn("Converting shared durable subscription '{}' to unshared " +
                    "(topicSubscriptionConversionEnabled=true)", info.getSubscriptionName());
            SharedSubscriptionKey sharedKey = new SharedSubscriptionKey(
                    context.getClientId(), info.getSubscriptionName());
            sharedDurableSubs.remove(sharedKey);
            durableSubscriptions.remove(subsKey);
        }
    }

    // [Policy]

    private void applyPolicy(ActiveMQDestination destination,
            SharedDurableTopicSubscription sub) {
        if (destination != null && broker.getDestinationPolicy() != null) {
            org.apache.activemq.broker.region.policy.PolicyEntry entry =
                    broker.getDestinationPolicy().getEntryFor(destination);
            if (entry != null) {
                entry.configure(broker, usageManager, sub);
            }
        }
    }

    private void applyPolicy(ActiveMQDestination destination,
            SharedTopicSubscription sub) {
        if (destination != null && broker.getDestinationPolicy() != null) {
            org.apache.activemq.broker.region.policy.PolicyEntry entry =
                    broker.getDestinationPolicy().getEntryFor(destination);
            if (entry != null) {
                entry.configurePrefetch(sub);
            }
        }
    }

    // [JMX hook] overridden by ManagedSharedTopicRegion

    protected void onSharedDurableReactivated(ConnectionContext context, Subscription sub) {
    }

    protected void onSharedNonDurableDestroyed(Subscription sub) {
    }

    @SuppressWarnings("unchecked")
    private void removeSubscriptionFromDestinations(ConnectionContext context,
            Subscription sub, ConsumerInfo info) throws Exception {
        destinationsLock.readLock().lock();
        try {
            for (Destination dest : (Set<Destination>) destinationMap.unsynchronizedGet(
                    info.getDestination())) {
                dest.removeSubscription(context, sub, info.getLastDeliveredSequenceId());
            }
        } finally {
            destinationsLock.readLock().unlock();
        }
    }

    private void validateSelectorMatch(ConsumerInfo incoming, ConsumerInfo existing)
            throws JMSException {
        String incomingSel = incoming.getSelector();
        String existingSel = existing.getSelector();
        if (incomingSel == null && existingSel == null) {
            return;
        }
        if (incomingSel == null || !incomingSel.equals(existingSel)) {
            throw new JMSException(
                    "Selector mismatch for shared subscription '" +
                    incoming.getSubscriptionName() +
                    "': existing='" + existingSel + "', incoming='" + incomingSel + "'",
                    ActiveMQErrorCode.SELECTOR_MISMATCH);
        }
    }

    private static void normalizeClientId(ConnectionContext context) {
        if (context.getClientId() == null) {
            context.setClientId("");
        }
    }

    private static String effectiveClientId(ConnectionContext context, SharedConsumerInfo info) {
        if (!info.isUserSpecifiedClientId()) {
            return "";
        }
        return context.getClientId() != null ? context.getClientId() : "";
    }
}
