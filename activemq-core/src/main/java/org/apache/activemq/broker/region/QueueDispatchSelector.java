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

import java.io.IOException;
import java.util.List;

import javax.jms.JMSException;

import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.broker.region.policy.SimpleDispatchSelector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Queue dispatch policy that determines if a message can be sent to a subscription
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class QueueDispatchSelector extends SimpleDispatchSelector {
    private static final Log LOG = LogFactory.getLog(QueueDispatchSelector.class);
    private Subscription exclusiveConsumer;
   
   
    /**
     * @param destination
     */
    public QueueDispatchSelector(ActiveMQDestination destination) {
        super(destination);
    }
    
    public Subscription getExclusiveConsumer() {
        return exclusiveConsumer;
    }
    public void setExclusiveConsumer(Subscription exclusiveConsumer) {
        this.exclusiveConsumer = exclusiveConsumer;
    }
    
    public boolean isExclusiveConsumer(Subscription s) {
        return s == this.exclusiveConsumer;
    }
    
       
    public boolean canSelect(Subscription subscription,
            MessageReference m) throws Exception {
       
        boolean result =  super.canDispatch(subscription, m);
        if (result && !subscription.isBrowser()) {
            result = exclusiveConsumer == null || exclusiveConsumer == subscription;
        }
        return result;
    }
    
}
