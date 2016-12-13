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
package org.apache.activemq.network;

import java.io.IOException;

import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.NetworkBridgeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidates subscriptions
 */
public class DurableConduitBridge extends ConduitBridge {
    private static final Logger LOG = LoggerFactory.getLogger(DurableConduitBridge.class);

    @Override
    public String toString() {
        return "DurableConduitBridge:" + configuration.getBrokerName() + "->" + getRemoteBrokerName();
    }
    /**
     * Constructor
     *
     * @param configuration
     *
     * @param localBroker
     * @param remoteBroker
     */
    public DurableConduitBridge(NetworkBridgeConfiguration configuration, Transport localBroker,
                                Transport remoteBroker) {
        super(configuration, localBroker, remoteBroker);
    }

    /**
     * Subscriptions for these destinations are always created
     *
     */
    @Override
    protected void setupStaticDestinations() {
        super.setupStaticDestinations();
        ActiveMQDestination[] dests = configuration.isDynamicOnly() ? null : durableDestinations;
        if (dests != null) {
            for (ActiveMQDestination dest : dests) {
                if (isPermissableDestination(dest) && !doesConsumerExist(dest)) {
                    try {
                        //Filtering by non-empty subscriptions, see AMQ-5875
                        if (dest.isTopic()) {
                            RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
                            TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();

                            String candidateSubName = getSubscriberName(dest);
                            for (Subscription subscription : topicRegion.getDurableSubscriptions().values()) {
                                String subName = subscription.getConsumerInfo().getSubscriptionName();
                                if (subName != null && subName.equals(candidateSubName)) {
                                    DemandSubscription sub = createDemandSubscription(dest, subName);
                                    sub.getLocalInfo().setSubscriptionName(getSubscriberName(dest));
                                    sub.setStaticallyIncluded(true);
                                    addSubscription(sub);
                                    break;
                                }
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("Failed to add static destination {}", dest, e);
                    }
                    LOG.trace("Forwarding messages for durable destination: {}", dest);
                } else if (configuration.isSyncDurableSubs() && !isPermissableDestination(dest)) {
                    if (dest.isTopic()) {
                        RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
                        TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();

                        String candidateSubName = getSubscriberName(dest);
                        for (Subscription subscription : topicRegion.getDurableSubscriptions().values()) {
                            String subName = subscription.getConsumerInfo().getSubscriptionName();
                            if (subName != null && subName.equals(candidateSubName) &&
                                    subscription instanceof DurableTopicSubscription) {
                               try {
                                    DurableTopicSubscription durableSub = (DurableTopicSubscription) subscription;
                                    //check the clientId so we only remove subs for the matching bridge
                                    if (durableSub.getSubscriptionKey().getClientId().equals(localClientId)) {
                                        // remove the NC subscription as it is no longer for a permissible dest
                                        RemoveSubscriptionInfo sending = new RemoveSubscriptionInfo();
                                        sending.setClientId(localClientId);
                                        sending.setSubscriptionName(subName);
                                        sending.setConnectionId(this.localConnectionInfo.getConnectionId());
                                        localBroker.oneway(sending);
                                    }
                                } catch (IOException e) {
                                    LOG.debug("Exception removing NC durable subscription: {}", subName, e);
                                    serviceRemoteException(e);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected DemandSubscription createDemandSubscription(ConsumerInfo info) throws IOException {
        boolean isForcedDurable = NetworkBridgeUtils.isForcedDurable(info,
                dynamicallyIncludedDestinations, staticallyIncludedDestinations);

        if (addToAlreadyInterestedConsumers(info, isForcedDurable)) {
            return null; // don't want this subscription added
        }
        //add our original id to ourselves
        info.addNetworkConsumerId(info.getConsumerId());
        ConsumerId forcedDurableId = isForcedDurable ? info.getConsumerId() : null;

        if(info.isDurable() || isForcedDurable) {
            // set the subscriber name to something reproducible
            info.setSubscriptionName(getSubscriberName(info.getDestination()));
            // and override the consumerId with something unique so that it won't
            // be removed if the durable subscriber (at the other end) goes away
            info.setConsumerId(new ConsumerId(localSessionInfo.getSessionId(),
                               consumerIdGenerator.getNextSequenceId()));
        }
        info.setSelector(null);
        DemandSubscription demandSubscription = doCreateDemandSubscription(info);
        if (forcedDurableId != null) {
            demandSubscription.addForcedDurableConsumer(forcedDurableId);
            forcedDurableRemoteId.add(forcedDurableId);
        }
        return demandSubscription;
    }

    protected String getSubscriberName(ActiveMQDestination dest) {
        String subscriberName = DURABLE_SUB_PREFIX + configuration.getBrokerName() + "_" + dest.getPhysicalName();
        return subscriberName;
    }

    protected boolean doesConsumerExist(ActiveMQDestination dest) {
        DestinationFilter filter = DestinationFilter.parseFilter(dest);
        for (DemandSubscription ds : subscriptionMapByLocalId.values()) {
            if (filter.matches(ds.getLocalInfo().getDestination())) {
                return true;
            }
        }
        return false;
    }
}
