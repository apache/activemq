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

import org.apache.activemq.broker.region.SharedDurableTopicSubscription;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionView;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.region.Subscription;

/**
 * JMX view for shared durable topic subscriptions. Delegates
 * shared-specific attributes to the underlying
 * {@link SharedDurableTopicSubscription}.
 */
public class SharedDurableSubscriptionView extends DurableSubscriptionView
        implements SharedDurableSubscriptionViewMBean {

    private final SharedDurableTopicSubscription sharedSub;

    public SharedDurableSubscriptionView(ManagedRegionBroker broker,
            BrokerService brokerService, String clientId, String userName,
            Subscription sub) {
        super(broker, brokerService, clientId, userName, sub);
        this.sharedSub = (SharedDurableTopicSubscription) sub;
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
        return "SharedDurableSubscriptionView: " + getClientId() + ":" + getSubscriptionName();
    }
}
