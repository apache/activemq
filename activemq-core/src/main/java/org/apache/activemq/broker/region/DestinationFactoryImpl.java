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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;

/**
 * Creates standard ActiveMQ implementations of
 * {@link org.apache.activemq.broker.region.Destination}.
 * 
 * @author fateev@amazon.com
 * @version $Revision$
 */
public class DestinationFactoryImpl extends DestinationFactory {

    protected final SystemUsage memoryManager;
    protected final TaskRunnerFactory taskRunnerFactory;
    protected final PersistenceAdapter persistenceAdapter;
    protected RegionBroker broker;

    public DestinationFactoryImpl(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, PersistenceAdapter persistenceAdapter) {
        this.memoryManager = memoryManager;
        this.taskRunnerFactory = taskRunnerFactory;
        if (persistenceAdapter == null) {
            throw new IllegalArgumentException("null persistenceAdapter");
        }
        this.persistenceAdapter = persistenceAdapter;
    }

    public void setRegionBroker(RegionBroker broker) {
        if (broker == null) {
            throw new IllegalArgumentException("null broker");
        }
        this.broker = broker;
    }

    public Set<ActiveMQDestination> getDestinations() {
        return persistenceAdapter.getDestinations();
    }

    /**
     * @return instance of {@link Queue} or {@link Topic}
     */
    public Destination createDestination(ConnectionContext context, ActiveMQDestination destination, DestinationStatistics destinationStatistics) throws Exception {
        if (destination.isQueue()) {
            if (destination.isTemporary()) {
                final ActiveMQTempDestination tempDest = (ActiveMQTempDestination)destination;
                return new Queue(broker.getRoot(), destination, memoryManager, null, destinationStatistics, taskRunnerFactory) {

                    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
                        // Only consumers on the same connection can consume
                        // from
                        // the temporary destination
                        if (!tempDest.getConnectionId().equals(sub.getConsumerInfo().getConsumerId().getConnectionId())) {
                            throw new JMSException("Cannot subscribe to remote temporary destination: " + tempDest);
                        }
                        super.addSubscription(context, sub);
                    };
                };
            } else {
                MessageStore store = persistenceAdapter.createQueueMessageStore((ActiveMQQueue)destination);
                Queue queue = new Queue(broker.getRoot(), destination, memoryManager, store, destinationStatistics, taskRunnerFactory);
                configureQueue(queue, destination);
                queue.initialize();
                return queue;
            }
        } else if (destination.isTemporary()) {
            final ActiveMQTempDestination tempDest = (ActiveMQTempDestination)destination;
            return new Topic(broker.getRoot(), destination, null, memoryManager, destinationStatistics, taskRunnerFactory) {

                public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
                    // Only consumers on the same connection can consume from
                    // the temporary destination
                    if (!tempDest.getConnectionId().equals(sub.getConsumerInfo().getConsumerId().getConnectionId())) {
                        throw new JMSException("Cannot subscribe to remote temporary destination: " + tempDest);
                    }
                    super.addSubscription(context, sub);
                };
            };
        } else {
            TopicMessageStore store = null;
            if (!AdvisorySupport.isAdvisoryTopic(destination)) {
                store = persistenceAdapter.createTopicMessageStore((ActiveMQTopic)destination);
            }
            Topic topic = new Topic(broker.getRoot(), destination, store, memoryManager, destinationStatistics, taskRunnerFactory);
            configureTopic(topic, destination);
            topic.initialize();
            return topic;
        }
    }

    protected void configureQueue(Queue queue, ActiveMQDestination destination) {
        if (broker == null) {
            throw new IllegalStateException("broker property is not set");
        }
        if (broker.getDestinationPolicy() != null) {
            PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
            if (entry != null) {
                entry.configure(broker,queue);
            }
        }
    }

    protected void configureTopic(Topic topic, ActiveMQDestination destination) {
        if (broker == null) {
            throw new IllegalStateException("broker property is not set");
        }
        if (broker.getDestinationPolicy() != null) {
            PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
            if (entry != null) {
                entry.configure(topic);
            }
        }
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        return persistenceAdapter.getLastMessageBrokerSequenceId();
    }

    public PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    public SubscriptionInfo[] getAllDurableSubscriptions(ActiveMQTopic topic) throws IOException {
        return persistenceAdapter.createTopicMessageStore(topic).getAllSubscriptions();
    }
}
