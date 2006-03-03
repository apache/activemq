/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.region.TopicSubscription;

/**
 * 
 * @version $Revision$
 */
public class TopicSubscriptionView extends SubscriptionView implements TopicSubscriptionViewMBean {

    public TopicSubscriptionView(String clientId, TopicSubscription subs) {
        super(clientId, subs);
    }

    protected TopicSubscription getTopicSubscription() {
        return (TopicSubscription) subscription;
    }

    /**
     * @return the number of messages discarded due to being a slow consumer
     */
    public int getDiscarded() {
        TopicSubscription topicSubscription = getTopicSubscription();
        return topicSubscription != null ? topicSubscription.discarded() : 0;
    }

    /**
     * @return the number of matched messages (messages targeted for the
     *         subscription but not yet able to be dispatched due to the
     *         prefetch buffer being full).
     */
    public int getMatched() {
        TopicSubscription topicSubscription = getTopicSubscription();
        return topicSubscription != null ? topicSubscription.matched() : 0;
    }
}
