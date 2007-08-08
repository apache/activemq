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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.transport.Transport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Iterator;
/**
 * Consolidates subscriptions
 * 
 * @version $Revision: 1.1 $
 */
public class DurableConduitBridge extends ConduitBridge{
    static final private Log log=LogFactory.getLog(DurableConduitBridge.class);

    /**
     * Constructor
     * @param configuration 
     * 
     * @param localBroker
     * @param remoteBroker
     */
    public DurableConduitBridge(NetworkBridgeConfiguration configuration,Transport localBroker,Transport remoteBroker){
        super(configuration,localBroker,remoteBroker);
    }

    /**
     * Subscriptions for these destinations are always created
     * 
     */
    protected void setupStaticDestinations(){
        super.setupStaticDestinations();
        ActiveMQDestination[] dests=durableDestinations;
        if(dests!=null){
            for(int i=0;i<dests.length;i++){
                ActiveMQDestination dest=dests[i];
                if(isPermissableDestination(dest) && !doesConsumerExist(dest)){
                    DemandSubscription sub=createDemandSubscription(dest);
                    if(dest.isTopic()){
                        sub.getLocalInfo().setSubscriptionName(getSubscriberName(dest));
                    }
                    try{
                        addSubscription(sub);
                    }catch(IOException e){
                        log.error("Failed to add static destination "+dest,e);
                    }
                    if(log.isTraceEnabled())
                        log.trace("Forwarding messages for durable destination: "+dest);
                }
            }
        }
    }

    protected DemandSubscription createDemandSubscription(ConsumerInfo info) throws IOException{
        if(addToAlreadyInterestedConsumers(info)){
            return null; // don't want this subscription added
        }
        // not matched so create a new one
        // but first, if it's durable - changed set the
        // ConsumerId here - so it won't be removed if the
        // durable subscriber goes away on the other end
        if(info.isDurable()||(info.getDestination().isQueue()&&!info.getDestination().isTemporary())){
            info.setConsumerId(new ConsumerId(localSessionInfo.getSessionId(),consumerIdGenerator.getNextSequenceId()));
        }
        if(info.isDurable()){
            // set the subscriber name to something reproducible
           
            info.setSubscriptionName(getSubscriberName(info.getDestination()));
        }
        return doCreateDemandSubscription(info);
    }
    
    protected String getSubscriberName(ActiveMQDestination dest){
        String subscriberName = configuration.getBrokerName()+"_"+dest.getPhysicalName();
        return subscriberName;
    }

    protected boolean doesConsumerExist(ActiveMQDestination dest){
        DestinationFilter filter=DestinationFilter.parseFilter(dest);
        for(Iterator i=subscriptionMapByLocalId.values().iterator();i.hasNext();){
            DemandSubscription ds=(DemandSubscription) i.next();
            if(filter.matches(ds.getLocalInfo().getDestination())){
                return true;
            }
        }
        return false;
    }
}
