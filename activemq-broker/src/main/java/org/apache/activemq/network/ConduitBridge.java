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
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidates subscriptions
 */
public class ConduitBridge extends DemandForwardingBridge {
    private static final Logger LOG = LoggerFactory.getLogger(ConduitBridge.class);

    /**
     * Constructor
     *
     * @param localBroker
     * @param remoteBroker
     */
    public ConduitBridge(NetworkBridgeConfiguration configuration, Transport localBroker, Transport remoteBroker) {
        super(configuration, localBroker, remoteBroker);
    }

    @Override
    protected DemandSubscription createDemandSubscription(ConsumerInfo info) throws IOException {
        if (addToAlreadyInterestedConsumers(info, false)) {
            return null; // don't want this subscription added
        }
        //add our original id to ourselves
        info.addNetworkConsumerId(info.getConsumerId());
        info.setSelector(null);
        return doCreateDemandSubscription(info);
    }

    protected boolean addToAlreadyInterestedConsumers(ConsumerInfo info, boolean isForcedDurable) {
        // search through existing subscriptions and see if we have a match
        if (info.isNetworkSubscription()) {
            return false;
        }
        boolean matched = false;

        for (DemandSubscription ds : subscriptionMapByLocalId.values()) {
            DestinationFilter filter = DestinationFilter.parseFilter(ds.getLocalInfo().getDestination());
            if (canConduit(ds) && filter.matches(info.getDestination())) {
                LOG.debug("{} {} with ids {} matched (add interest) {}", new Object[]{
                        configuration.getBrokerName(), info, info.getNetworkConsumerIds(), ds
                });
                // add the interest in the subscription
                if (!info.isDurable()) {
                    ds.add(info.getConsumerId());
                    if (isForcedDurable) {
                        forcedDurableRemoteId.add(info.getConsumerId());
                        ds.addForcedDurableConsumer(info.getConsumerId());
                    }
                } else {
                    ds.getDurableRemoteSubs().add(new SubscriptionInfo(info.getClientId(), info.getSubscriptionName()));
                }
                matched = true;
                // continue - we want interest to any existing DemandSubscriptions
            }
        }
        return matched;
    }

    // we want to conduit statically included consumers which are local networkSubs
    // but we don't want to conduit remote network subs i.e. (proxy proxy) consumers
    private boolean canConduit(DemandSubscription ds) {
        return ds.isStaticallyIncluded() || !ds.getRemoteInfo().isNetworkSubscription();
    }

    @Override
    protected void removeDemandSubscription(ConsumerId id) throws IOException {
        List<DemandSubscription> tmpList = new ArrayList<DemandSubscription>();

        for (DemandSubscription ds : subscriptionMapByLocalId.values()) {
            if (ds.remove(id)) {
                LOG.debug("{} on {} from {} removed interest for: {} from {}", new Object[]{
                        configuration.getBrokerName(), localBroker, remoteBrokerName, id, ds
                });
            }
            if (ds.isEmpty()) {
                tmpList.add(ds);
            }
        }

        for (DemandSubscription ds : tmpList) {
            removeSubscription(ds);
            LOG.debug("{} on {} from {} removed {}", new Object[]{
                    configuration.getBrokerName(), localBroker, remoteBrokerName, ds
            });
        }
    }
}
