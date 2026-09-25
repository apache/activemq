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
package org.apache.activemq.broker;

import java.io.IOException;

import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;

import org.apache.activemq.annotation.Experimental;
import org.apache.activemq.broker.jmx.ManagedSharedTopicRegion;
import org.apache.activemq.broker.region.SharedTopicRegion;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SharedConsumerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;

/**
 * Extends {@link BrokerService} to install a {@link SharedTopicRegion}
 * that supports JMS 3.1 shared topic subscriptions.
 *
 * <p>Use this class in place of {@code BrokerService} in XML configuration
 * to enable shared subscription support. In Spring/XBean configuration this
 * is the {@code <sharedTopicBrokerService>} element of the
 * {@code http://activemq.apache.org/schema/core} namespace, and it accepts
 * every attribute {@code <broker>} does.
 *
 * @org.apache.xbean.XBean
 *
 */
@Experimental("Tech Preview for JMS 3.1 shared topic subscriptions")
public class SharedTopicBrokerService extends BrokerService {

    private static final Logger LOG = LoggerFactory.getLogger(SharedTopicBrokerService.class);
    private static final int SHARED_STORE_OPENWIRE_VERSION = 13;

    private boolean topicSubscriptionConversionEnabled;

    public SharedTopicBrokerService() {
        setStoreOpenWireVersion(SHARED_STORE_OPENWIRE_VERSION);
    }

    public boolean isTopicSubscriptionConversionEnabled() {
        return topicSubscriptionConversionEnabled;
    }

    public void setTopicSubscriptionConversionEnabled(boolean topicSubscriptionConversionEnabled) {
        this.topicSubscriptionConversionEnabled = topicSubscriptionConversionEnabled;
    }

    @Override
    protected Broker createRegionBroker(
            org.apache.activemq.broker.region.DestinationInterceptor destinationInterceptor)
            throws IOException {

        RegionBroker regionBroker;
        if (isUseJmx()) {
            try {
                regionBroker = new ManagedRegionBroker(this, getManagementContext(),
                        getBrokerObjectName(), getTaskRunnerFactory(), getConsumerSystemUsage(),
                        destinationFactory, destinationInterceptor, getScheduler(),
                        getExecutor()) {
                    @Override
                    protected Region createTopicRegion(SystemUsage memoryManager,
                            TaskRunnerFactory taskRunnerFactory,
                            DestinationFactory df) {
                        ManagedSharedTopicRegion region = new ManagedSharedTopicRegion(
                                this, destinationStatistics, memoryManager,
                                taskRunnerFactory, df);
                        region.setTopicSubscriptionConversionEnabled(
                                topicSubscriptionConversionEnabled);
                        return region;
                    }

                    @Override
                    public void removeConsumer(ConnectionContext context,
                            ConsumerInfo info) throws Exception {
                        if (info instanceof SharedConsumerInfo
                                && ((SharedConsumerInfo) info).isShared()) {
                            ActiveMQDestination dest = info.getDestination();
                            Region region = getRegion(dest);
                            Subscription sub = null;
                            if (region instanceof org.apache.activemq.broker.region.AbstractRegion) {
                                sub = ((org.apache.activemq.broker.region.AbstractRegion) region)
                                        .getSubscriptions().get(info.getConsumerId());
                            }
                            region.removeConsumer(context, info);
                            if (sub != null && sub instanceof DurableTopicSubscription
                                    && !((DurableTopicSubscription) sub).isActive()) {
                                ObjectName name = sub.getObjectName();
                                if (name != null) {
                                    unregisterSubscription(name, true);
                                }
                            }
                        } else {
                            super.removeConsumer(context, info);
                        }
                    }

                    @Override
                    public void unregisterSubscription(Subscription sub) {
                        ObjectName name = sub.getObjectName();
                        if (name != null) {
                            try {
                                unregisterSubscription(name, false);
                            } catch (Exception e) {
                                LOG.warn("Failed to unregister shared subscription MBean: {}",
                                        e.getMessage(), e);
                            }
                        }
                        super.unregisterSubscription(sub);
                    }
                };
            } catch (MalformedObjectNameException me) {
                LOG.warn("Cannot create ManagedRegionBroker due {}", me.getMessage(), me);
                throw new IOException(me);
            }
        } else {
            regionBroker = new RegionBroker(this, getTaskRunnerFactory(),
                    getConsumerSystemUsage(), destinationFactory, destinationInterceptor,
                    getScheduler(), getExecutor()) {
                @Override
                protected Region createTopicRegion(SystemUsage memoryManager,
                        TaskRunnerFactory taskRunnerFactory,
                        DestinationFactory df) {
                    SharedTopicRegion region = new SharedTopicRegion(this,
                            destinationStatistics, memoryManager, taskRunnerFactory, df);
                    region.setTopicSubscriptionConversionEnabled(
                            topicSubscriptionConversionEnabled);
                    return region;
                }
            };
        }

        destinationFactory.setRegionBroker(regionBroker);
        regionBroker.setKeepDurableSubsActive(isKeepDurableSubsActive());
        regionBroker.getDestinationStatistics().setEnabled(isEnableStatistics());
        regionBroker.setAllowTempAutoCreationOnSend(isAllowTempAutoCreationOnSend());
        return regionBroker;
    }
}
