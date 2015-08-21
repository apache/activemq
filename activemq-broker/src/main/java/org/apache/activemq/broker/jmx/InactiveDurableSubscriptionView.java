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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SubscriptionInfo;

/**
 *
 *
 */
public class InactiveDurableSubscriptionView extends DurableSubscriptionView implements DurableSubscriptionViewMBean {
    protected SubscriptionInfo subscriptionInfo;

    /**
     * Constructor
     *
     * @param broker
     * @param brokerService
     * @param clientId
     * @param subInfo
     * @param subscription
     */
    public InactiveDurableSubscriptionView(ManagedRegionBroker broker, BrokerService brokerService, String clientId, SubscriptionInfo subInfo, Subscription subscription) {
        super(broker, brokerService, clientId, null, subscription);
        this.broker = broker;
        this.subscriptionInfo = subInfo;
    }

    /**
     * @return the id of the Subscription
     */
    @Override
    public long getSubscriptionId() {
        return -1;
    }

    /**
     * @return the destination name
     */
    @Override
    public String getDestinationName() {
        return subscriptionInfo.getDestination().getPhysicalName();
    }

    /**
     * @return true if the destination is a Queue
     */
    @Override
    public boolean isDestinationQueue() {
        return false;
    }

    /**
     * @return true of the destination is a Topic
     */
    @Override
    public boolean isDestinationTopic() {
        return true;
    }

    /**
     * @return true if the destination is temporary
     */
    @Override
    public boolean isDestinationTemporary() {
        return false;
    }

    /**
     * @return name of the durable consumer
     */
    @Override
    public String getSubscriptionName() {
        return subscriptionInfo.getSubscriptionName();
    }

    /**
     * @return true if the subscriber is active
     */
    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    protected ConsumerInfo getConsumerInfo() {
        // when inactive, consumer info is stale
        return null;
    }

    /**
     * Browse messages for this durable subscriber
     *
     * @return messages
     * @throws OpenDataException
     */
    @Override
    public CompositeData[] browse() throws OpenDataException {
        return broker.browse(this);
    }

    /**
     * Browse messages for this durable subscriber
     *
     * @return messages
     * @throws OpenDataException
     */
    @Override
    public TabularData browseAsTable() throws OpenDataException {
        return broker.browseAsTable(this);
    }

    /**
     * Destroys the durable subscription so that messages will no longer be
     * stored for this subscription
     */
    @Override
    public void destroy() throws Exception {
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubscriptionName(subscriptionInfo.getSubscriptionName());
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        brokerService.getBroker().removeSubscription(context, info);
    }

    @Override
    public String toString() {
        return "InactiveDurableSubscriptionView: " + getClientId() + ":" + getSubscriptionName();
    }

    @Override
    public String getSelector() {
        return subscriptionInfo.getSelector();
    }

    @Override
    public void removeMessage(@MBeanInfo("messageId") String messageId) throws Exception {
        broker.remove(this, messageId);
    }

}
