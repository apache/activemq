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
package org.apache.activemq.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.command.SubscriptionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a network bridge interface
 */
public class DemandSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(DemandSubscription.class);

    private final ConsumerInfo remoteInfo;
    private final ConsumerInfo localInfo;
    private final Set<ConsumerId> remoteSubsIds = new CopyOnWriteArraySet<ConsumerId>();
    private final AtomicInteger dispatched = new AtomicInteger(0);
    private final AtomicBoolean activeWaiter = new AtomicBoolean();
    private final Set<SubscriptionInfo> durableRemoteSubs = new CopyOnWriteArraySet<SubscriptionInfo>();
    private final Set<ConsumerId> forcedDurableConsumers = new CopyOnWriteArraySet<ConsumerId>();
    private SubscriptionInfo localDurableSubscriber;

    private NetworkBridgeFilter networkBridgeFilter;
    private boolean staticallyIncluded;

    DemandSubscription(ConsumerInfo info) {
        remoteInfo = info;
        localInfo = info.copy();
        localInfo.setNetworkSubscription(true);
        remoteSubsIds.add(info.getConsumerId());
    }

    @Override
    public String toString() {
        return "DemandSub{" + localInfo.getConsumerId() + ",remotes:" + remoteSubsIds + "}";
    }

    /**
     * Increment the consumers associated with this subscription
     *
     * @param id
     * @return true if added
     */
    public boolean add(ConsumerId id) {
        return remoteSubsIds.add(id);
    }

    /**
     * Increment the consumers associated with this subscription
     *
     * @param id
     * @return true if removed
     */
    public boolean remove(ConsumerId id) {
        return remoteSubsIds.remove(id);
    }

    public Set<SubscriptionInfo> getDurableRemoteSubs() {
        return durableRemoteSubs;
    }

    /**
     * @return true if there are no interested consumers
     */
    public boolean isEmpty() {
        return remoteSubsIds.isEmpty();
    }

    public int size() {
        return remoteSubsIds.size();
    }
    /**
     * @return Returns the localInfo.
     */
    public ConsumerInfo getLocalInfo() {
        return localInfo;
    }

    /**
     * @return Returns the remoteInfo.
     */
    public ConsumerInfo getRemoteInfo() {
        return remoteInfo;
    }

    public boolean addForcedDurableConsumer(ConsumerId id) {
        return forcedDurableConsumers.add(id);
    }

    public boolean removeForcedDurableConsumer(ConsumerId id) {
        return forcedDurableConsumers.remove(id);
    }

    public int getForcedDurableConsumersSize() {
        return forcedDurableConsumers.size();
    }

    public void waitForCompletion() {
        if (dispatched.get() > 0) {
            LOG.debug("Waiting for completion for sub: {}, dispatched: {}", localInfo.getConsumerId(), this.dispatched.get());
            activeWaiter.set(true);
            if (dispatched.get() > 0) {
                synchronized (activeWaiter) {
                    try {
                        activeWaiter.wait(TimeUnit.SECONDS.toMillis(30));
                    } catch (InterruptedException ignored) {
                    }
                }
                if (this.dispatched.get() > 0) {
                    LOG.warn("demand sub interrupted or timedout while waiting for outstanding responses, expect potentially {} duplicate forwards", this.dispatched.get());
                }
            }
        }
    }

    public void decrementOutstandingResponses() {
        if (dispatched.decrementAndGet() == 0 && activeWaiter.get()) {
            synchronized (activeWaiter) {
                activeWaiter.notifyAll();
            }
        }
    }

    public boolean incrementOutstandingResponses() {
        dispatched.incrementAndGet();
        if (activeWaiter.get()) {
            decrementOutstandingResponses();
            return false;
        }
        return true;
    }

    public NetworkBridgeFilter getNetworkBridgeFilter() {
        return networkBridgeFilter;
    }

    public void setNetworkBridgeFilter(NetworkBridgeFilter networkBridgeFilter) {
        this.networkBridgeFilter = networkBridgeFilter;
    }

    public SubscriptionInfo getLocalDurableSubscriber() {
        return localDurableSubscriber;
    }

    public void setLocalDurableSubscriber(SubscriptionInfo localDurableSubscriber) {
        this.localDurableSubscriber = localDurableSubscriber;
    }

    public boolean isStaticallyIncluded() {
        return staticallyIncluded;
    }

    public void setStaticallyIncluded(boolean staticallyIncluded) {
        this.staticallyIncluded = staticallyIncluded;
    }
}
