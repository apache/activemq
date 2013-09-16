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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transport.InactivityIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abort slow consumers when they reach the configured threshold of slowness, default is slow for 30 seconds
 *
 * @org.apache.xbean.XBean
 */
public class AbortSlowConsumerStrategy implements SlowConsumerStrategy, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AbortSlowConsumerStrategy.class);

    protected String name = "AbortSlowConsumerStrategy@" + hashCode();
    protected Scheduler scheduler;
    protected Broker broker;
    protected final AtomicBoolean taskStarted = new AtomicBoolean(false);
    protected final Map<Subscription, SlowConsumerEntry> slowConsumers =
        new ConcurrentHashMap<Subscription, SlowConsumerEntry>();

    private long maxSlowCount = -1;
    private long maxSlowDuration = 30*1000;
    private long checkPeriod = 30*1000;
    private boolean abortConnection = false;

    @Override
    public void setBrokerService(Broker broker) {
       this.scheduler = broker.getScheduler();
       this.broker = broker;
    }

    @Override
    public void slowConsumer(ConnectionContext context, Subscription subs) {
        if (maxSlowCount < 0 && maxSlowDuration < 0) {
            // nothing to do
            LOG.info("no limits set, slowConsumer strategy has nothing to do");
            return;
        }

        if (taskStarted.compareAndSet(false, true)) {
            scheduler.executePeriodically(this, checkPeriod);
        }

        if (!slowConsumers.containsKey(subs)) {
            slowConsumers.put(subs, new SlowConsumerEntry(context));
        } else if (maxSlowCount > 0) {
            slowConsumers.get(subs).slow();
        }
    }

    @Override
    public void run() {
        if (maxSlowDuration > 0) {
            // mark
            for (SlowConsumerEntry entry : slowConsumers.values()) {
                entry.mark();
            }
        }

        HashMap<Subscription, SlowConsumerEntry> toAbort = new HashMap<Subscription, SlowConsumerEntry>();
        for (Entry<Subscription, SlowConsumerEntry> entry : slowConsumers.entrySet()) {
            if (entry.getKey().isSlowConsumer()) {
                if (maxSlowDuration > 0 && (entry.getValue().markCount * checkPeriod >= maxSlowDuration)
                        || maxSlowCount > 0 && entry.getValue().slowCount >= maxSlowCount) {
                    toAbort.put(entry.getKey(), entry.getValue());
                    slowConsumers.remove(entry.getKey());
                }
            } else {
                LOG.info("sub: " + entry.getKey().getConsumerInfo().getConsumerId() + " is no longer slow");
                slowConsumers.remove(entry.getKey());
            }
        }

        abortSubscription(toAbort, abortConnection);
    }

    protected void abortSubscription(Map<Subscription, SlowConsumerEntry> toAbort, boolean abortSubscriberConnection) {

        Map<Connection, List<Subscription>> abortMap = new HashMap<Connection, List<Subscription>>();

        for (final Entry<Subscription, SlowConsumerEntry> entry : toAbort.entrySet()) {
            ConnectionContext connectionContext = entry.getValue().context;
            if (connectionContext == null) {
                continue;
            }

            Connection connection = connectionContext.getConnection();
            if (connection == null) {
                LOG.debug("slowConsumer abort ignored, no connection in context:"  + connectionContext);
            }

            if (!abortMap.containsKey(connection)) {
                abortMap.put(connection, new ArrayList<Subscription>());
            }

            abortMap.get(connection).add(entry.getKey());
        }

        for (Entry<Connection, List<Subscription>> entry : abortMap.entrySet()) {
            final Connection connection = entry.getKey();
            final List<Subscription> subscriptions = entry.getValue();

            if (abortSubscriberConnection) {

                LOG.info("aborting connection:{} with {} slow consumers",
                         connection.getConnectionId(), subscriptions.size());

                if (LOG.isTraceEnabled()) {
                    for (Subscription subscription : subscriptions) {
                        LOG.trace("Connection {} being aborted because of slow consumer: {} on destination: {}",
                                  new Object[] { connection.getConnectionId(),
                                                 subscription.getConsumerInfo().getConsumerId(),
                                                 subscription.getActiveMQDestination() });
                    }
                }

                try {
                    scheduler.executeAfterDelay(new Runnable() {
                        @Override
                        public void run() {
                            connection.serviceException(new InactivityIOException(
                                    subscriptions.size() + " Consumers was slow too often (>"
                                    + maxSlowCount +  ") or too long (>"
                                    + maxSlowDuration + "): "));
                        }}, 0l);
                } catch (Exception e) {
                    LOG.info("exception on aborting connection {} with {} slow consumers",
                             connection.getConnectionId(), subscriptions.size());
                }
            } else {
                // just abort each consumer by telling it to stop
                for (Subscription subscription : subscriptions) {
                    LOG.info("aborting slow consumer: {} for destination:{}",
                             subscription.getConsumerInfo().getConsumerId(),
                             subscription.getActiveMQDestination());

                    try {
                        ConsumerControl stopConsumer = new ConsumerControl();
                        stopConsumer.setConsumerId(subscription.getConsumerInfo().getConsumerId());
                        stopConsumer.setClose(true);
                        connection.dispatchAsync(stopConsumer);
                    } catch (Exception e) {
                        LOG.info("exception on aborting slow consumer: {}", subscription.getConsumerInfo().getConsumerId());
                    }
                }
            }
        }
    }

    public void abortConsumer(Subscription sub, boolean abortSubscriberConnection) {
        if (sub != null) {
            SlowConsumerEntry entry = slowConsumers.remove(sub);
            if (entry != null) {
                Map<Subscription, SlowConsumerEntry> toAbort = new HashMap<Subscription, SlowConsumerEntry>();
                toAbort.put(sub, entry);
                abortSubscription(toAbort, abortSubscriberConnection);
            } else {
                LOG.warn("cannot abort subscription as it no longer exists in the map of slow consumers: " + sub);
            }
        }
    }

    public long getMaxSlowCount() {
        return maxSlowCount;
    }

    /**
     * number of times a subscription can be deemed slow before triggering abort
     * effect depends on dispatch rate as slow determination is done on dispatch
     */
    public void setMaxSlowCount(long maxSlowCount) {
        this.maxSlowCount = maxSlowCount;
    }

    public long getMaxSlowDuration() {
        return maxSlowDuration;
    }

    /**
     * time in milliseconds that a sub can remain slow before triggering
     * an abort.
     * @param maxSlowDuration
     */
    public void setMaxSlowDuration(long maxSlowDuration) {
        this.maxSlowDuration = maxSlowDuration;
    }

    public long getCheckPeriod() {
        return checkPeriod;
    }

    /**
     * time in milliseconds between checks for slow subscriptions
     * @param checkPeriod
     */
    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

    public boolean isAbortConnection() {
        return abortConnection;
    }

    /**
     * abort the consumers connection rather than sending a stop command to the remote consumer
     * @param abortConnection
     */
    public void setAbortConnection(boolean abortConnection) {
        this.abortConnection = abortConnection;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Map<Subscription, SlowConsumerEntry> getSlowConsumers() {
        return slowConsumers;
    }

    @Override
    public void addDestination(Destination destination) {
        // Not needed for this strategy.
    }
}
