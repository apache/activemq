/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.broker.region.policy;

import org.activemq.broker.ConnectionContext;
import org.activemq.broker.region.MessageReference;
import org.activemq.broker.region.Subscription;
import org.activemq.broker.region.Topic;
import org.activemq.filter.MessageEvaluationContext;

/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will only keep 
 * the last message.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class LastImageSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    volatile private MessageReference lastImage;

    public boolean add(ConnectionContext context, MessageReference node) throws Throwable {
        lastImage = node;
        return true;
    }

    public void recover(ConnectionContext context, Topic topic, Subscription sub) throws Throwable {
        // Re-dispatch the last message seen.
        MessageReference node = lastImage;
        if( node != null ){
            MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
            try {
                msgContext.setDestination(node.getRegionDestination().getActiveMQDestination());
                msgContext.setMessageReference(node);                        
                if (sub.matches(node, msgContext)) {
                    sub.add(node);
                }
            } finally {
                msgContext.clear();
            }
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

}
