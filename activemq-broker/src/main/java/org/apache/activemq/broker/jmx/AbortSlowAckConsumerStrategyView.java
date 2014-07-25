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

import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;

public class AbortSlowAckConsumerStrategyView extends AbortSlowConsumerStrategyView implements AbortSlowAckConsumerStrategyViewMBean {

    private final AbortSlowAckConsumerStrategy strategy;

    public AbortSlowAckConsumerStrategyView(ManagedRegionBroker managedRegionBroker, AbortSlowAckConsumerStrategy slowConsumerStrategy) {
        super(managedRegionBroker, slowConsumerStrategy);
        this.strategy = slowConsumerStrategy;
    }

    @Override
    public long getMaxTimeSinceLastAck() {
        return strategy.getMaxTimeSinceLastAck();
    }

    @Override
    public void setMaxTimeSinceLastAck(long maxTimeSinceLastAck) {
        this.strategy.setMaxTimeSinceLastAck(maxTimeSinceLastAck);
    }

    @Override
    public boolean isIgnoreIdleConsumers() {
        return strategy.isIgnoreIdleConsumers();
    }

    @Override
    public void setIgnoreIdleConsumers(boolean ignoreIdleConsumers) {
        this.strategy.setIgnoreIdleConsumers(ignoreIdleConsumers);
    }

    @Override
    public boolean isIgnoreNetworkConsumers() {
        return this.strategy.isIgnoreNetworkSubscriptions();
    }

    @Override
    public void setIgnoreNetworkConsumers(boolean ignoreNetworkConsumers) {
        this.strategy.setIgnoreNetworkConsumers(ignoreNetworkConsumers);
    }
}
