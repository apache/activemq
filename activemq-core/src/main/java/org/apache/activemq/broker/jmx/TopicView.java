/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;

public class TopicView extends DestinationView implements TopicViewMBean{
    
     public TopicView(ManagedRegionBroker broker, Topic destination){
        super(broker, destination);
    }

    public void createDurableSubscriber(String clientId,String subscriberName) throws Exception{
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        ConsumerInfo info = new ConsumerInfo();
        ConsumerId consumerId = new ConsumerId();
        consumerId.setConnectionId(clientId);
        consumerId.setSessionId(0);
        consumerId.setValue(0);
        info.setConsumerId(consumerId);
        info.setDestination(destination.getActiveMQDestination());
        info.setSubcriptionName(subscriberName);
        broker.addConsumer(context, info);
        broker.removeConsumer(context, info);        
    }

    public void destroyDurableSubscriber(String clientId,String subscriberName) throws Exception{
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubcriptionName(subscriberName);
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        broker.removeSubscription(context, info);
  
        
    }
}
