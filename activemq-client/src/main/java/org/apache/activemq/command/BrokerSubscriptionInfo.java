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
package org.apache.activemq.command;

import org.apache.activemq.state.CommandVisitor;

/**
 * Used to represent the durable subscriptions contained by the broker
 * This is used to synchronize durable subs on bridge creation
 *
 * @openwire:marshaller code="92"
 *
 */
public class BrokerSubscriptionInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.BROKER_SUBSCRIPTION_INFO;

    BrokerId brokerId;
    String brokerName;
    ConsumerInfo subscriptionInfos[];

    public BrokerSubscriptionInfo() {

    }

    public BrokerSubscriptionInfo(String brokerName) {
        this.brokerName = brokerName;
    }

    public BrokerSubscriptionInfo copy() {
        BrokerSubscriptionInfo copy = new BrokerSubscriptionInfo();
        copy(copy);
        return copy;
    }

    private void copy(BrokerSubscriptionInfo copy) {
        super.copy(copy);
        copy.subscriptionInfos = this.subscriptionInfos;
        copy.brokerName = this.brokerName;
        copy.brokerId = this.brokerId;
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processBrokerSubscriptionInfo(this);
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=12
     */
    public BrokerId getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(BrokerId brokerId) {
        this.brokerId = brokerId;
    }

    /**
     * @openwire:property version=12
     */
    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    /**
     * @openwire:property version=12
     */
    public ConsumerInfo[] getSubscriptionInfos() {
        return subscriptionInfos;
    }

    public void setSubscriptionInfos(ConsumerInfo[] subscriptionInfos) {
        this.subscriptionInfos = subscriptionInfos;
    }

}
