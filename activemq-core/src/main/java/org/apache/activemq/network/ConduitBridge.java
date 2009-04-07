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
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Consolidates subscriptions
 * 
 * @version $Revision: 1.1 $
 */
public class ConduitBridge extends DemandForwardingBridge {
    private static final Log LOG = LogFactory.getLog(ConduitBridge.class);

    /**
     * Constructor
     * 
     * @param localBroker
     * @param remoteBroker
     */
    public ConduitBridge(NetworkBridgeConfiguration configuration, Transport localBroker, Transport remoteBroker) {
        super(configuration, localBroker, remoteBroker);
    }

    protected DemandSubscription createDemandSubscription(ConsumerInfo info) throws IOException {
        if (addToAlreadyInterestedConsumers(info)) {
            return null; // don't want this subscription added
        }
        //add our original id to ourselves
        info.addNetworkConsumerId(info.getConsumerId());
        info.setSelector(null);
        return doCreateDemandSubscription(info);
    }

    protected boolean addToAlreadyInterestedConsumers(ConsumerInfo info) {
        // search through existing subscriptions and see if we have a match
        boolean matched = false;
        DestinationFilter filter = DestinationFilter.parseFilter(info.getDestination());
        for (Iterator i = subscriptionMapByLocalId.values().iterator(); i.hasNext();) {
            DemandSubscription ds = (DemandSubscription)i.next();
            if (filter.matches(ds.getLocalInfo().getDestination())) {
                // add the interest in the subscription
                // ds.add(ds.getRemoteInfo().getConsumerId());
                ds.add(info.getConsumerId());
                matched = true;
                // continue - we want interest to any existing
                // DemandSubscriptions
            }
        }
        return matched;
    }

    protected void removeDemandSubscription(ConsumerId id) throws IOException {
        List<DemandSubscription> tmpList = new ArrayList<DemandSubscription>();

        for (Iterator i = subscriptionMapByLocalId.values().iterator(); i.hasNext();) {
            DemandSubscription ds = (DemandSubscription)i.next();
            ds.remove(id);
            if (ds.isEmpty()) {
                tmpList.add(ds);
            }
        }
        for (Iterator<DemandSubscription> i = tmpList.iterator(); i.hasNext();) {
            DemandSubscription ds = i.next();
            subscriptionMapByLocalId.remove(ds.getRemoteInfo().getConsumerId());
            removeSubscription(ds);
            if (LOG.isDebugEnabled()) {
                LOG.debug(configuration.getBrokerName() + " removing sub on " + localBroker + " from " + remoteBrokerName + " :  " + ds.getRemoteInfo());
            }
        }

    }

}
