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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consolidates subscriptions
 * 
 * @version $Revision: 1.1 $
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
        if (addToAlreadyInterestedConsumers(info)) {
            return null; // don't want this subscription added
        }
        //add our original id to ourselves
        info.addNetworkConsumerId(info.getConsumerId());
        info.setSelector(null);
        return doCreateDemandSubscription(info);
    }
    
    protected boolean checkPaths(BrokerId[] first, BrokerId[] second) {
    	if (first == null || second == null)
			return true;
		if (Arrays.equals(first, second))
			return true;
		if (first[0].equals(second[0])
				&& first[first.length - 1].equals(second[second.length - 1]))
			return false;
		else
			return true;
    }

    protected boolean addToAlreadyInterestedConsumers(ConsumerInfo info) {
        // search through existing subscriptions and see if we have a match
        boolean matched = false;
        for (Iterator i = subscriptionMapByLocalId.values().iterator(); i.hasNext();) {
            DemandSubscription ds = (DemandSubscription)i.next();
            DestinationFilter filter = DestinationFilter.parseFilter(ds.getLocalInfo().getDestination());
            if (filter.matches(info.getDestination())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(configuration.getBrokerName() + " matched (add interest) to exsting sub for: " + ds.getRemoteInfo()
                            + " with sub: " + info.getConsumerId());
                }
                // add the interest in the subscription
                // ds.add(ds.getRemoteInfo().getConsumerId());
                if (checkPaths(info.getBrokerPath(), ds.getRemoteInfo().getBrokerPath())) {
                	ds.add(info.getConsumerId());
                }
                matched = true;
                // continue - we want interest to any existing
                // DemandSubscriptions
            }
        }
        return matched;
    }

    @Override
    protected void removeDemandSubscription(ConsumerId id) throws IOException {
        List<DemandSubscription> tmpList = new ArrayList<DemandSubscription>();

        for (Iterator i = subscriptionMapByLocalId.values().iterator(); i.hasNext();) {
            DemandSubscription ds = (DemandSubscription)i.next();
            if (ds.remove(id)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(configuration.getBrokerName() + " removing interest in sub on " + localBroker + " from " + remoteBrokerName + " : sub: " + id  + " existing matched sub: " + ds.getRemoteInfo());
                }
            }
            if (ds.isEmpty()) {
                tmpList.add(ds);
            }
        }
        for (Iterator<DemandSubscription> i = tmpList.iterator(); i.hasNext();) {
            DemandSubscription ds = i.next();
            removeSubscription(ds);
            if (LOG.isDebugEnabled()) {
                LOG.debug(configuration.getBrokerName() + " removing sub on " + localBroker + " from " + remoteBrokerName + " :  " + ds.getRemoteInfo());
            }
        }

    }

}
