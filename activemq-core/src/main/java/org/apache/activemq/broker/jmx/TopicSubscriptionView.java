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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.TopicSubscription;

/**
 * 
 * 
 */
public class TopicSubscriptionView extends SubscriptionView implements TopicSubscriptionViewMBean {

    public TopicSubscriptionView(String clientId, TopicSubscription subs) {
        super(clientId, subs);
    }

    protected TopicSubscription getTopicSubscription() {
        return (TopicSubscription)subscription;
    }

    /**
     * @return the number of messages discarded due to being a slow consumer
     */
    public int getDiscardedCount() {
        TopicSubscription topicSubscription = getTopicSubscription();
        return topicSubscription != null ? topicSubscription.discarded() : 0;
    }

    /**
     * @return the maximun number of messages that can be pending.
     */
    public int getMaximumPendingQueueSize() {
        TopicSubscription topicSubscription = getTopicSubscription();
        return topicSubscription != null ? topicSubscription.getMaximumPendingMessages() : 0;
    }

    /**
     * 
     */
    public void setMaximumPendingQueueSize(int max) {
        TopicSubscription topicSubscription = getTopicSubscription();
        if (topicSubscription != null) {
            topicSubscription.setMaximumPendingMessages(max);
        }
    }

    @Override
    public boolean isActive() {
        if (subscription instanceof DurableTopicSubscription) {
            return ((DurableTopicSubscription)subscription).isActive();
        } else {
            return super.isActive();
        }
    }

    

}
