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

import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.SlowConsumerEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.util.Map;

public class AbortSlowConsumerStrategyView implements AbortSlowConsumerStrategyViewMBean {
    private static final Logger LOG = LoggerFactory.getLogger(AbortSlowConsumerStrategyView.class);
    private ManagedRegionBroker broker;
    private AbortSlowConsumerStrategy strategy;


    public AbortSlowConsumerStrategyView(ManagedRegionBroker managedRegionBroker, AbortSlowConsumerStrategy slowConsumerStrategy) {
        this.broker = managedRegionBroker;
        this.strategy = slowConsumerStrategy;
    }

    public long getMaxSlowCount() {
        return strategy.getMaxSlowCount();
    }

    public void setMaxSlowCount(long maxSlowCount) {
        strategy.setMaxSlowCount(maxSlowCount);
    }

    public long getMaxSlowDuration() {
        return strategy.getMaxSlowDuration();
    }

    public void setMaxSlowDuration(long maxSlowDuration) {
       strategy.setMaxSlowDuration(maxSlowDuration);
    }

    public long getCheckPeriod() {
        return strategy.getCheckPeriod();
    }

    public TabularData getSlowConsumers() throws OpenDataException {

        OpenTypeSupport.OpenTypeFactory factory = OpenTypeSupport.getFactory(SlowConsumerEntry.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("SlowConsumers", "Table of current slow Consumers", ct, new String[] {"subscription" });
        TabularDataSupport rc = new TabularDataSupport(tt);
        
        int index = 0;
        Map<Subscription, SlowConsumerEntry> slowConsumers = strategy.getSlowConsumers();
        for (Map.Entry<Subscription, SlowConsumerEntry> entry : slowConsumers.entrySet()) {
            entry.getValue().setSubscription(broker.getSubscriberObjectName(entry.getKey()));
            rc.put(OpenTypeSupport.convert(entry.getValue()));
        }
        return rc;
    }

    public void abortConsumer(ObjectName consumerToAbort) {
        Subscription sub = broker.getSubscriber(consumerToAbort);
        if (sub != null) {
            LOG.info("aborting consumer via jmx: " + sub.getConsumerInfo().getConsumerId());           
            strategy.abortConsumer(sub, false);
        } else {
            LOG.warn("cannot resolve subscription matching name: " + consumerToAbort);
        }

    }

    public void abortConnection(ObjectName consumerToAbort) {
        Subscription sub = broker.getSubscriber(consumerToAbort);
        if (sub != null) {
            LOG.info("aborting consumer connection via jmx: " + sub.getConsumerInfo().getConsumerId().getConnectionId());
            strategy.abortConsumer(sub, true);
        } else {
            LOG.warn("cannot resolve subscription matching name: " + consumerToAbort);
        }
    }

    public void abortConsumer(String objectNameOfConsumerToAbort) {
        abortConsumer(toObjectName(objectNameOfConsumerToAbort));
    }

    public void abortConnection(String objectNameOfConsumerToAbort) {
        abortConnection(toObjectName(objectNameOfConsumerToAbort));
    }

    private ObjectName toObjectName(String objectName) {
        ObjectName result = null;
        try {
            result = new ObjectName(objectName);
        } catch (Exception e) {
            LOG.warn("cannot create subscription ObjectName to abort, from string: " + objectName);
        }
        return result;
    }
}
