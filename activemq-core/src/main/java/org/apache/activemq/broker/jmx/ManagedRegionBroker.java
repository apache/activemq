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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.OpenTypeSupport.OpenTypeFactory;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DestinationFactoryImpl;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ManagedRegionBroker extends RegionBroker {
    private static final Log LOG = LogFactory.getLog(ManagedRegionBroker.class);
    private final MBeanServer mbeanServer;
    private final ObjectName brokerObjectName;
    private final Map<ObjectName, DestinationView> topics = new ConcurrentHashMap<ObjectName, DestinationView>();
    private final Map<ObjectName, DestinationView> queues = new ConcurrentHashMap<ObjectName, DestinationView>();
    private final Map<ObjectName, DestinationView> temporaryQueues = new ConcurrentHashMap<ObjectName, DestinationView>();
    private final Map<ObjectName, DestinationView> temporaryTopics = new ConcurrentHashMap<ObjectName, DestinationView>();
    private final Map<ObjectName, SubscriptionView> queueSubscribers = new ConcurrentHashMap<ObjectName, SubscriptionView>();
    private final Map<ObjectName, SubscriptionView> topicSubscribers = new ConcurrentHashMap<ObjectName, SubscriptionView>();
    private final Map<ObjectName, SubscriptionView> durableTopicSubscribers = new ConcurrentHashMap<ObjectName, SubscriptionView>();
    private final Map<ObjectName, SubscriptionView> inactiveDurableTopicSubscribers = new ConcurrentHashMap<ObjectName, SubscriptionView>();
    private final Map<ObjectName, SubscriptionView> temporaryQueueSubscribers = new ConcurrentHashMap<ObjectName, SubscriptionView>();
    private final Map<ObjectName, SubscriptionView> temporaryTopicSubscribers = new ConcurrentHashMap<ObjectName, SubscriptionView>();
    private final Map<SubscriptionKey, ObjectName> subscriptionKeys = new ConcurrentHashMap<SubscriptionKey, ObjectName>();
    private final Map<Subscription, ObjectName> subscriptionMap = new ConcurrentHashMap<Subscription, ObjectName>();
    private final Set<ObjectName> registeredMBeans = new CopyOnWriteArraySet<ObjectName>();
    /* This is the first broker in the broker interceptor chain. */
    private Broker contextBroker;

    public ManagedRegionBroker(BrokerService brokerService, MBeanServer mbeanServer, ObjectName brokerObjectName, TaskRunnerFactory taskRunnerFactory, UsageManager memoryManager,
                               DestinationFactory destinationFactory, DestinationInterceptor destinationInterceptor) throws IOException {
        super(brokerService, taskRunnerFactory, memoryManager, destinationFactory, destinationInterceptor);
        this.mbeanServer = mbeanServer;
        this.brokerObjectName = brokerObjectName;
    }

    public void start() throws Exception {
        super.start();
        // build all existing durable subscriptions
        buildExistingSubscriptions();
    }

    protected void doStop(ServiceStopper stopper) {
        super.doStop(stopper);
        // lets remove any mbeans not yet removed
        for (Iterator<ObjectName> iter = registeredMBeans.iterator(); iter.hasNext();) {
            ObjectName name = iter.next();
            try {
                mbeanServer.unregisterMBean(name);
            } catch (InstanceNotFoundException e) {
                LOG.warn("The MBean: " + name + " is no longer registered with JMX");
            } catch (Exception e) {
                stopper.onException(this, e);
            }
        }
        registeredMBeans.clear();
    }

    protected Region createQueueRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new ManagedQueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTempQueueRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new ManagedTempQueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTempTopicRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new ManagedTempTopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTopicRegion(UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new ManagedTopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    public void register(ActiveMQDestination destName, Destination destination) {
        // TODO refactor to allow views for custom destinations
        try {
            ObjectName objectName = createObjectName(destName);
            DestinationView view;
            if (destination instanceof Queue) {
                view = new QueueView(this, (Queue)destination);
            } else if (destination instanceof Topic) {
                view = new TopicView(this, (Topic)destination);
            } else {
                view = null;
                LOG.warn("JMX View is not supported for custom destination: " + destination);
            }
            if (view != null) {
                registerDestination(objectName, destName, view);
            }
        } catch (Exception e) {
            LOG.error("Failed to register destination " + destName, e);
        }
    }

    public void unregister(ActiveMQDestination destName) {
        try {
            ObjectName objectName = createObjectName(destName);
            unregisterDestination(objectName);
        } catch (Exception e) {
            LOG.error("Failed to unregister " + destName, e);
        }
    }

    public ObjectName registerSubscription(ConnectionContext context, Subscription sub) {
        Hashtable map = brokerObjectName.getKeyPropertyList();
        String objectNameStr = brokerObjectName.getDomain() + ":" + "BrokerName=" + map.get("BrokerName") + ",Type=Subscription,";
        String destinationType = "destinationType=" + sub.getConsumerInfo().getDestination().getDestinationTypeAsString();
        String destinationName = "destinationName=" + JMXSupport.encodeObjectNamePart(sub.getConsumerInfo().getDestination().getPhysicalName());
        String clientId = "clientId=" + JMXSupport.encodeObjectNamePart(context.getClientId());
        String persistentMode = "persistentMode=";
        String consumerId = "";
        SubscriptionKey key = new SubscriptionKey(context.getClientId(), sub.getConsumerInfo().getSubscriptionName());
        if (sub.getConsumerInfo().isDurable()) {
            persistentMode += "Durable, subscriptionID=" + JMXSupport.encodeObjectNamePart(sub.getConsumerInfo().getSubscriptionName());
        } else {
            persistentMode += "Non-Durable";
            if (sub.getConsumerInfo() != null && sub.getConsumerInfo().getConsumerId() != null) {
                consumerId = ",consumerId=" + JMXSupport.encodeObjectNamePart(sub.getConsumerInfo().getConsumerId().toString());
            }
        }
        objectNameStr += persistentMode + ",";
        objectNameStr += destinationType + ",";
        objectNameStr += destinationName + ",";
        objectNameStr += clientId;
        objectNameStr += consumerId;
        try {
            ObjectName objectName = new ObjectName(objectNameStr);
            SubscriptionView view;
            if (sub.getConsumerInfo().isDurable()) {
                view = new DurableSubscriptionView(this, context.getClientId(), sub);
            } else {
                if (sub instanceof TopicSubscription) {
                    view = new TopicSubscriptionView(context.getClientId(), (TopicSubscription)sub);
                } else {
                    view = new SubscriptionView(context.getClientId(), sub);
                }
            }
            registerSubscription(objectName, sub.getConsumerInfo(), key, view);
            subscriptionMap.put(sub, objectName);
            return objectName;
        } catch (Exception e) {
            LOG.error("Failed to register subscription " + sub, e);
            return null;
        }
    }

    public void unregisterSubscription(Subscription sub) {
        ObjectName name = subscriptionMap.remove(sub);
        if (name != null) {
            try {
                unregisterSubscription(name);
            } catch (Exception e) {
                LOG.error("Failed to unregister subscription " + sub, e);
            }
        }
    }

    protected void registerDestination(ObjectName key, ActiveMQDestination dest, DestinationView view) throws Exception {
        if (dest.isQueue()) {
            if (dest.isTemporary()) {
                temporaryQueues.put(key, view);
            } else {
                queues.put(key, view);
            }
        } else {
            if (dest.isTemporary()) {
                temporaryTopics.put(key, view);
            } else {
                topics.put(key, view);
            }
        }
        try {
            mbeanServer.registerMBean(view, key);
            registeredMBeans.add(key);
        } catch (Throwable e) {
            LOG.warn("Failed to register MBean: " + key);
            LOG.debug("Failure reason: " + e, e);
        }
    }

    protected void unregisterDestination(ObjectName key) throws Exception {
        topics.remove(key);
        queues.remove(key);
        temporaryQueues.remove(key);
        temporaryTopics.remove(key);
        if (registeredMBeans.remove(key)) {
            try {
                mbeanServer.unregisterMBean(key);
            } catch (Throwable e) {
                LOG.warn("Failed to unregister MBean: " + key);
                LOG.debug("Failure reason: " + e, e);
            }
        }
    }

    protected void registerSubscription(ObjectName key, ConsumerInfo info, SubscriptionKey subscriptionKey, SubscriptionView view) throws Exception {
        ActiveMQDestination dest = info.getDestination();
        if (dest.isQueue()) {
            if (dest.isTemporary()) {
                temporaryQueueSubscribers.put(key, view);
            } else {
                queueSubscribers.put(key, view);
            }
        } else {
            if (dest.isTemporary()) {
                temporaryTopicSubscribers.put(key, view);
            } else {
                if (info.isDurable()) {
                    durableTopicSubscribers.put(key, view);
                    // unregister any inactive durable subs
                    try {
                        ObjectName inactiveName = subscriptionKeys.get(subscriptionKey);
                        if (inactiveName != null) {
                            inactiveDurableTopicSubscribers.remove(inactiveName);
                            registeredMBeans.remove(inactiveName);
                            mbeanServer.unregisterMBean(inactiveName);
                        }
                    } catch (Throwable e) {
                        LOG.error("Unable to unregister inactive durable subscriber: " + subscriptionKey, e);
                    }
                } else {
                    topicSubscribers.put(key, view);
                }
            }
        }

        try {
            mbeanServer.registerMBean(view, key);
            registeredMBeans.add(key);
        } catch (Throwable e) {
            LOG.warn("Failed to register MBean: " + key);
            LOG.debug("Failure reason: " + e, e);
        }

    }

    protected void unregisterSubscription(ObjectName key) throws Exception {
        queueSubscribers.remove(key);
        topicSubscribers.remove(key);
        inactiveDurableTopicSubscribers.remove(key);
        temporaryQueueSubscribers.remove(key);
        temporaryTopicSubscribers.remove(key);
        if (registeredMBeans.remove(key)) {
            try {
                mbeanServer.unregisterMBean(key);
            } catch (Throwable e) {
                LOG.warn("Failed to unregister MBean: " + key);
                LOG.debug("Failure reason: " + e, e);
            }
        }
        DurableSubscriptionView view = (DurableSubscriptionView)durableTopicSubscribers.remove(key);
        if (view != null) {
            // need to put this back in the inactive list
            SubscriptionKey subscriptionKey = new SubscriptionKey(view.getClientId(), view.getSubscriptionName());
            SubscriptionInfo info = new SubscriptionInfo();
            info.setClientId(subscriptionKey.getClientId());
            info.setSubcriptionName(subscriptionKey.getSubscriptionName());
            info.setDestination(new ActiveMQTopic(view.getDestinationName()));
            addInactiveSubscription(subscriptionKey, info);
        }
    }

    protected void buildExistingSubscriptions() throws Exception {
        Map<SubscriptionKey, SubscriptionInfo> subscriptions = new HashMap<SubscriptionKey, SubscriptionInfo>();
        Set destinations = destinationFactory.getDestinations();
        if (destinations != null) {
            for (Iterator iter = destinations.iterator(); iter.hasNext();) {
                ActiveMQDestination dest = (ActiveMQDestination)iter.next();
                if (dest.isTopic()) {
                    SubscriptionInfo[] infos = destinationFactory.getAllDurableSubscriptions((ActiveMQTopic)dest);
                    if (infos != null) {
                        for (int i = 0; i < infos.length; i++) {
                            SubscriptionInfo info = infos[i];
                            LOG.debug("Restoring durable subscription: " + info);
                            SubscriptionKey key = new SubscriptionKey(info);
                            subscriptions.put(key, info);
                        }
                    }
                }
            }
        }
        for (Iterator i = subscriptions.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            SubscriptionKey key = (SubscriptionKey)entry.getKey();
            SubscriptionInfo info = (SubscriptionInfo)entry.getValue();
            addInactiveSubscription(key, info);
        }
    }

    protected void addInactiveSubscription(SubscriptionKey key, SubscriptionInfo info) {
        Hashtable map = brokerObjectName.getKeyPropertyList();
        try {
            ObjectName objectName = new ObjectName(brokerObjectName.getDomain() + ":" + "BrokerName=" + map.get("BrokerName") + "," + "Type=Subscription," + "active=false,"
                                                   + "name=" + JMXSupport.encodeObjectNamePart(key.toString()) + "");
            SubscriptionView view = new InactiveDurableSubscriptionView(this, key.getClientId(), info);

            try {
                mbeanServer.registerMBean(view, objectName);
                registeredMBeans.add(objectName);
            } catch (Throwable e) {
                LOG.warn("Failed to register MBean: " + key);
                LOG.debug("Failure reason: " + e, e);
            }

            inactiveDurableTopicSubscribers.put(objectName, view);
            subscriptionKeys.put(key, objectName);
        } catch (Exception e) {
            LOG.error("Failed to register subscription " + info, e);
        }
    }

    public CompositeData[] browse(SubscriptionView view) throws OpenDataException {
        List<Message> messages = getSubscriberMessages(view);
        CompositeData c[] = new CompositeData[messages.size()];
        for (int i = 0; i < c.length; i++) {
            try {
                c[i] = OpenTypeSupport.convert(messages.get(i));
            } catch (Throwable e) {
                LOG.error("failed to browse : " + view, e);
            }
        }
        return c;
    }

    public TabularData browseAsTable(SubscriptionView view) throws OpenDataException {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(ActiveMQMessage.class);
        List<Message> messages = getSubscriberMessages(view);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("MessageList", "MessageList", ct, new String[] {"JMSMessageID"});
        TabularDataSupport rc = new TabularDataSupport(tt);
        for (int i = 0; i < messages.size(); i++) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(messages.get(i))));
        }
        return rc;
    }

    protected List<Message> getSubscriberMessages(SubscriptionView view) {
        // TODO It is very dangerous operation for big backlogs
        if (!(destinationFactory instanceof DestinationFactoryImpl)) {
            throw new RuntimeException("unsupported by " + destinationFactory);
        }
        PersistenceAdapter adapter = ((DestinationFactoryImpl)destinationFactory).getPersistenceAdapter();
        final List<Message> result = new ArrayList<Message>();
        try {
            ActiveMQTopic topic = new ActiveMQTopic(view.getDestinationName());
            TopicMessageStore store = adapter.createTopicMessageStore(topic);
            store.recover(new MessageRecoveryListener() {
                public boolean recoverMessage(Message message) throws Exception {
                    result.add(message);
                    return true;
                }

                public boolean recoverMessageReference(MessageId messageReference) throws Exception {
                    throw new RuntimeException("Should not be called.");
                }

                public void finished() {
                }

                public boolean hasSpace() {
                    return true;
                }
            });
        } catch (Throwable e) {
            LOG.error("Failed to browse messages for Subscription " + view, e);
        }
        return result;

    }

    protected ObjectName[] getTopics() {
        Set<ObjectName> set = topics.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getQueues() {
        Set<ObjectName> set = queues.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryTopics() {
        Set<ObjectName> set = temporaryTopics.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryQueues() {
        Set<ObjectName> set = temporaryQueues.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTopicSubscribers() {
        Set<ObjectName> set = topicSubscribers.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getDurableTopicSubscribers() {
        Set<ObjectName> set = durableTopicSubscribers.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getQueueSubscribers() {
        Set<ObjectName> set = queueSubscribers.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryTopicSubscribers() {
        Set<ObjectName> set = temporaryTopicSubscribers.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryQueueSubscribers() {
        Set<ObjectName> set = temporaryQueueSubscribers.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getInactiveDurableTopicSubscribers() {
        Set<ObjectName> set = inactiveDurableTopicSubscribers.keySet();
        return set.toArray(new ObjectName[set.size()]);
    }

    public Broker getContextBroker() {
        return contextBroker;
    }

    public void setContextBroker(Broker contextBroker) {
        this.contextBroker = contextBroker;
    }

    protected ObjectName createObjectName(ActiveMQDestination destName) throws MalformedObjectNameException {
        // Build the object name for the destination
        Hashtable map = brokerObjectName.getKeyPropertyList();
        ObjectName objectName = new ObjectName(brokerObjectName.getDomain() + ":" + "BrokerName=" + map.get("BrokerName") + "," + "Type="
                                               + JMXSupport.encodeObjectNamePart(destName.getDestinationTypeAsString()) + "," + "Destination="
                                               + JMXSupport.encodeObjectNamePart(destName.getPhysicalName()));
        return objectName;
    }
}
