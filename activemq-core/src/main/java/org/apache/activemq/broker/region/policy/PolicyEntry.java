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
package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.RedeliveryPolicy;
import org.apache.activemq.filter.DestinationMapEntry;

/**
 * Represents an entry in a {@link PolicyMap} for assigning policies to a
 * specific destination or a hierarchical wildcard area of destinations.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision: 1.1 $
 */
public class PolicyEntry extends DestinationMapEntry {

    private DispatchPolicy dispatchPolicy;
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy;
    private RedeliveryPolicy redeliveryPolicy;
    private boolean sendAdvisoryIfNoConsumers;

    public void configure(Queue queue) {
        if (dispatchPolicy != null) {
            queue.setDispatchPolicy(dispatchPolicy);
        }
    }

    public void configure(Topic topic) {
        if (dispatchPolicy != null) {
            topic.setDispatchPolicy(dispatchPolicy);
        }
        if (subscriptionRecoveryPolicy != null) {
            topic.setSubscriptionRecoveryPolicy(subscriptionRecoveryPolicy);
        }
        topic.setSendAdvisoryIfNoConsumers(sendAdvisoryIfNoConsumers);
    }

    // Properties
    // -------------------------------------------------------------------------
    public DispatchPolicy getDispatchPolicy() {
        return dispatchPolicy;
    }

    public void setDispatchPolicy(DispatchPolicy policy) {
        this.dispatchPolicy = policy;
    }

    public RedeliveryPolicy getRedeliveryPolicy() {
        return redeliveryPolicy;
    }

    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicy = redeliveryPolicy;
    }

    public SubscriptionRecoveryPolicy getSubscriptionRecoveryPolicy() {
        return subscriptionRecoveryPolicy;
    }

    public void setSubscriptionRecoveryPolicy(SubscriptionRecoveryPolicy subscriptionRecoveryPolicy) {
        this.subscriptionRecoveryPolicy = subscriptionRecoveryPolicy;
    }

    public boolean isSendAdvisoryIfNoConsumers() {
        return sendAdvisoryIfNoConsumers;
    }

    /**
     * Sends an advisory message if a non-persistent message is sent and there
     * are no active consumers
     */
    public void setSendAdvisoryIfNoConsumers(boolean sendAdvisoryIfNoConsumers) {
        this.sendAdvisoryIfNoConsumers = sendAdvisoryIfNoConsumers;
    }
}
