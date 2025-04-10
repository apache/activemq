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
package org.apache.activemq.util;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerSubscriptionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.network.NetworkBridgeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.network.NetworkBridgeConfiguration.DURABLE_SUB_PREFIX;

public class NetworkBridgeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBridgeUtils.class);

    /**
     * Generate the BrokerSubscriptionInfo which is used to tell the broker on the other
     * side of the network bridge which NC durable subscriptions are still needed for demand.
     * @param brokerService
     * @param config
     * @return
     */
    public static BrokerSubscriptionInfo getBrokerSubscriptionInfo(final BrokerService brokerService,
            final NetworkBridgeConfiguration config) {

        RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
        TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        Set<ConsumerInfo> subscriptionInfos = new HashSet<>();

        // Add all durable subscriptions to the set that match the network config
        // which currently is just the dynamicallyIncludedDestinations list
        for (Map.Entry<SubscriptionKey, DurableTopicSubscription> entry : topicRegion.getDurableSubscriptions().entrySet()) {
            final SubscriptionKey key = entry.getKey();
            final DurableTopicSubscription sub = entry.getValue();
            // We must use the key for matchesConfigForDurableSync() because the clientId for the ConsumerInfo object
            // may be null if the subscription is offline, which is why we copy the ConsumerInfo below
            // and set the clientId to match the key. The correct clientId is important for the receiving broker
            // to do proper filtering when TTL is set
            if (sub != null && NetworkBridgeUtils.matchesConfigForDurableSync(config, key.getClientId(),
                key.getSubscriptionName(), sub.getActiveMQDestination())) {
                ConsumerInfo ci = sub.getConsumerInfo().copy();
                ci.setClientId(key.getClientId());
                subscriptionInfos.add(ci);
            }
        }

        // We also need to iterate over all normal subscriptions and check if they are part of
        // any dynamicallyIncludedDestination that is configured with forceDurable to be true
        // over the network bridge.  If forceDurable is true then we want to add the consumer to the set
        for (Subscription sub : topicRegion.getSubscriptions().values()) {
            if (sub != null && NetworkBridgeUtils.isForcedDurable(sub.getConsumerInfo(),
                config.getDynamicallyIncludedDestinations())) {
                subscriptionInfos.add(sub.getConsumerInfo().copy());
            }
        }

        try {
            // Lastly, if isUseVirtualDestSubs is configured on this broker (to fire advisories) and
            // configured on the network connector (to listen to advisories) then also add any virtual
            // dest subscription to the set if forceDurable is true for its destination
            AdvisoryBroker ab = (AdvisoryBroker) brokerService.getBroker().getAdaptor(AdvisoryBroker.class);
            if (ab != null && brokerService.isUseVirtualDestSubs() && config.isUseVirtualDestSubs()) {
                for (ConsumerInfo info : ab.getVirtualDestinationConsumers().keySet()) {
                    if (NetworkBridgeUtils.isForcedDurable(info, config.getDynamicallyIncludedDestinations())) {
                        subscriptionInfos.add(info.copy());
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Error processing virtualDestinationSubs for BrokerSubscriptionInfo");
            LOG.debug("Error processing virtualDestinationSubs for BrokerSubscriptionInfo", e);
        }
        BrokerSubscriptionInfo bsi = new BrokerSubscriptionInfo(brokerService.getBrokerName());
        bsi.setSubscriptionInfos(subscriptionInfos.toArray(new ConsumerInfo[0]));
        return bsi;
    }

    public static boolean isForcedDurable(final ConsumerInfo info,
                                          final List<ActiveMQDestination> dynamicallyIncludedDestinations) {
        return dynamicallyIncludedDestinations != null && isForcedDurable(info,
            dynamicallyIncludedDestinations.toArray(new ActiveMQDestination[0]), null);
    }

    public static boolean isForcedDurable(final ConsumerInfo info,
            final ActiveMQDestination[] dynamicallyIncludedDestinations,
            final ActiveMQDestination[] staticallyIncludedDestinations) {

        if (info.isDurable() || info.getDestination().isQueue()) {
            return false;
        }

        ActiveMQDestination destination = info.getDestination();
        if (AdvisorySupport.isAdvisoryTopic(destination) || destination.isTemporary() ||
            destination.isQueue()) {
            return false;
        }

        ActiveMQDestination matching = findMatchingDestination(dynamicallyIncludedDestinations, destination);
        if (matching != null) {
            return isDestForcedDurable(matching);
        }
        matching = findMatchingDestination(staticallyIncludedDestinations, destination);
        if (matching != null) {
            return isDestForcedDurable(matching);
        }
        return false;
    }

    /**
     * This method is used to determine which durable subscriptions should be sent from
     * a broker to a remote broker so that the remote broker can process the subscriptions
     * to re-add demand when the bridge is first started during the durable sync phase
     * of a bridge starting. We can cut down on the amount of durables sent/processed
     * based on how the bridge is configured.
     *
     * @param config
     * @param clientId
     * @param subscriptionName
     * @param destination
     * @return
     */
    public static boolean matchesConfigForDurableSync(final NetworkBridgeConfiguration config,
        String clientId, String subscriptionName, ActiveMQDestination destination) {

        // If consumerTTL was set to 0 then we return false because no demand will be
        // generated over the bridge as the messages will be limited to the local broker
        if (config.getConsumerTTL() == 0) {
            return false;
        }

        // If this is a remote demand consumer for the current bridge we can also skip
        // This consumer was created by another consumer on the remote broker, so we
        // ignore this consumer for demand, or else we'd end up with a loop
        if (isDirectBridgeConsumer(config, clientId, subscriptionName)) {
            return false;
        }

        // if TTL is set to 1 then we won't ever handle proxy durables as they
        // are at least 2 hops away so the TTL check would always fail. Proxy durables
        // are subs for other bridges, so we can skip these as well.
        if (config.getConsumerTTL() == 1 && isProxyBridgeSubscription(config, clientId,
            subscriptionName)) {
            return false;
        }

        // Verify the destination matches the dynamically included destination list
        return matchesDestinations(config.getDynamicallyIncludedDestinations(), destination);
    }

    public static boolean matchesDestination(ActiveMQDestination destFilter, ActiveMQDestination destToMatch) {
        DestinationFilter inclusionFilter = DestinationFilter.parseFilter(destFilter);
        return inclusionFilter.matches(destToMatch) && destFilter.getDestinationType() == destToMatch.getDestinationType();
    }

    public static boolean matchesDestinations(final List<ActiveMQDestination> includedDests, final ActiveMQDestination destination) {
        if (includedDests != null && !includedDests.isEmpty()) {
            for (ActiveMQDestination dest : includedDests) {
                if (matchesDestination(dest, destination)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static ActiveMQDestination findMatchingDestination(ActiveMQDestination[] dests, ActiveMQDestination destination) {
        if (dests != null) {
            for (ActiveMQDestination dest : dests) {
                if (matchesDestination(dest, destination)) {
                    return dest;
                }
            }
        }
        return null;
    }

    public static boolean isDestForcedDurable(final ActiveMQDestination destination) {
        boolean isForceDurable = false;
        if (destination != null) {
            final Map<String, String> options = destination.getOptions();

            if (options != null) {
                isForceDurable = (boolean) TypeConversionSupport.convert(options.get("forceDurable"), boolean.class);
            }
        }

        return isForceDurable;
    }

    public static boolean isDirectBridgeConsumer(NetworkBridgeConfiguration config, String clientId, String subName) {
        return (subName != null && subName.startsWith(DURABLE_SUB_PREFIX)) &&
            (clientId == null || clientId.startsWith(config.getName()));
    }

    public static boolean isProxyBridgeSubscription(NetworkBridgeConfiguration config, String clientId, String subName) {
        if (subName != null && clientId != null) {
            return subName.startsWith(DURABLE_SUB_PREFIX) && !clientId.startsWith(config.getName());
        }
        return false;
    }
}
