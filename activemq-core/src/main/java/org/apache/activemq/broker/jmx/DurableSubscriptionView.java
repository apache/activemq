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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.RemoveSubscriptionInfo;

/**
 * @version $Revision: 1.5 $
 */
public class DurableSubscriptionView extends SubscriptionView implements DurableSubscriptionViewMBean {

    protected ManagedRegionBroker broker;
    protected String subscriptionName;

    /**
     * Constructor
     * 
     * @param clientId
     * @param sub
     */
    public DurableSubscriptionView(ManagedRegionBroker broker, String clientId, Subscription sub) {
        super(clientId, sub);
        this.broker = broker;
        this.subscriptionName = sub.getConsumerInfo().getSubscriptionName();
    }

    /**
     * @return name of the durable consumer
     */
    public String getSubscriptionName() {
        return subscriptionName;
    }

    /**
     * Browse messages for this durable subscriber
     * 
     * @return messages
     * @throws OpenDataException
     */
    public CompositeData[] browse() throws OpenDataException {
        return broker.browse(this);
    }

    /**
     * Browse messages for this durable subscriber
     * 
     * @return messages
     * @throws OpenDataException
     */
    public TabularData browseAsTable() throws OpenDataException {
        return broker.browseAsTable(this);
    }

    /**
     * Destroys the durable subscription so that messages will no longer be
     * stored for this subscription
     */
    public void destroy() throws Exception {
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubscriptionName(subscriptionName);
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        broker.removeSubscription(context, info);
    }

    public String toString() {
        return "InactiveDurableSubscriptionView: " + getClientId() + ":" + getSubscriptionName();
    }
}
