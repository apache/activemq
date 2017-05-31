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

        //Add all durable subscriptions to the set that match the network config
        //which currently is just the dynamicallyIncludedDestinations list
        for (SubscriptionKey key : topicRegion.getDurableSubscriptions().keySet()) {
            DurableTopicSubscription sub = topicRegion.getDurableSubscriptions().get(key);
            if (sub != null && NetworkBridgeUtils.matchesNetworkConfig(config, sub.getConsumerInfo().getDestination())) {
                ConsumerInfo ci = sub.getConsumerInfo().copy();
                ci.setClientId(key.getClientId());
                subscriptionInfos.add(ci);
            }
        }

        //We also need to iterate over all normal subscriptions and check if they are part of
        //any dynamicallyIncludedDestination that is configured with forceDurable to be true
        //over the network bridge.  If forceDurable is true then we want to add the consumer to the set
        for (Subscription sub : topicRegion.getSubscriptions().values()) {
            if (sub != null && NetworkBridgeUtils.isForcedDurable(sub.getConsumerInfo(),
                    config.getDynamicallyIncludedDestinations())) {
                subscriptionInfos.add(sub.getConsumerInfo().copy());
            }
        }

        try {
            //Lastly, if isUseVirtualDestSubs is configured on this broker (to fire advisories) and
            //configured on the network connector (to listen to advisories) then also add any virtual
            //dest subscription to the set if forceDurable is true for its destination
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
        return dynamicallyIncludedDestinations != null
                ? isForcedDurable(info,
                        dynamicallyIncludedDestinations.toArray(new ActiveMQDestination[0]), null) : false;
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

    public static boolean matchesNetworkConfig(final NetworkBridgeConfiguration config,
            ActiveMQDestination destination) {
        List<ActiveMQDestination> includedDests = config.getDynamicallyIncludedDestinations();
        if (includedDests != null && includedDests.size() > 0) {
            for (ActiveMQDestination dest : includedDests) {
                DestinationFilter inclusionFilter = DestinationFilter.parseFilter(dest);
                if (dest != null && inclusionFilter.matches(destination) && dest.getDestinationType() == destination.getDestinationType()) {
                    return true;
                }
            }
        }

        return false;
    }

    public static boolean matchesDestinations(ActiveMQDestination[] dests, final ActiveMQDestination destination) {
        if (dests != null && dests.length > 0) {
            for (ActiveMQDestination dest : dests) {
                DestinationFilter inclusionFilter = DestinationFilter.parseFilter(dest);
                if (dest != null && inclusionFilter.matches(destination) && dest.getDestinationType() == destination.getDestinationType()) {
                    return true;
                }
            }
        }

        return false;
    }

    public static ActiveMQDestination findMatchingDestination(ActiveMQDestination[] dests, ActiveMQDestination destination) {
        if (dests != null && dests.length > 0) {
            for (ActiveMQDestination dest : dests) {
                DestinationFilter inclusionFilter = DestinationFilter.parseFilter(dest);
                if (dest != null && inclusionFilter.matches(destination) && dest.getDestinationType() == destination.getDestinationType()) {
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
}
