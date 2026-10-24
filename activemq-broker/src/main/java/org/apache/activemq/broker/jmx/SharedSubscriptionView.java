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

import org.apache.activemq.broker.region.SharedTopicSubscription;
import org.apache.activemq.broker.jmx.SubscriptionView;
import org.apache.activemq.broker.region.Subscription;

/**
 * JMX view for shared non-durable topic subscriptions. Delegates
 * shared-specific attributes to the underlying
 * {@link SharedTopicSubscription}.
 */
public class SharedSubscriptionView extends SubscriptionView
        implements SharedSubscriptionViewMBean {

    private final SharedTopicSubscription sharedSub;

    public SharedSubscriptionView(String clientId, String userName, Subscription sub) {
        super(clientId, userName, sub);
        this.sharedSub = (SharedTopicSubscription) sub;
    }

    @Override
    public boolean isShared() {
        return true;
    }

    @Override
    public int getConsumerCount() {
        return sharedSub.getConsumerCount();
    }

    @Override
    public String toString() {
        return "SharedSubscriptionView: " + getClientId() + ":" + getSubscriptionName();
    }
}
