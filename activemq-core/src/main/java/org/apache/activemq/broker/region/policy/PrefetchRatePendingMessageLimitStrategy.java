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
package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.region.TopicSubscription;

/**
 * This PendingMessageLimitStrategy sets the maximum pending message limit value to be
 * a multiplier of the prefetch limit of the subscription.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class PrefetchRatePendingMessageLimitStrategy implements PendingMessageLimitStrategy {

    private double multiplier = 0.5;

    public int getMaximumPendingMessageLimit(TopicSubscription subscription) {
        int prefetchSize = subscription.getConsumerInfo().getPrefetchSize();
        return (int) (prefetchSize * multiplier);
    }

    public double getMultiplier() {
        return multiplier;
    }

    /**
     * Sets the multiplier of the prefetch size which will be used to define the maximum number of pending
     * messages for non-durable topics before messages are discarded.
     */
    public void setMultiplier(double rate) {
        this.multiplier = rate;
    }

}
