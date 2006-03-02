/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.JMXSupport;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
public class ManagedRegionBroker extends RegionBroker{
    private static final Log log=LogFactory.getLog(ManagedRegionBroker.class);
    private final MBeanServer mbeanServer;
    private final ObjectName brokerObjectName;
    private final Map topics=new ConcurrentHashMap();
    private final Map queues=new ConcurrentHashMap();
    private final Map temporaryQueues=new ConcurrentHashMap();
    private final Map temporaryTopics=new ConcurrentHashMap();
    private final Map queueSubscribers=new ConcurrentHashMap();
    private final Map topicSubscribers=new ConcurrentHashMap();
    private final Map durableTopicSubscribers=new ConcurrentHashMap();
    private final Map inactiveDurableTopicSubscribers=new ConcurrentHashMap();
    private final Map temporaryQueueSubscribers=new ConcurrentHashMap();
    private final Map temporaryTopicSubscribers=new ConcurrentHashMap();
    private final Map subscriptionKeys = new ConcurrentHashMap();
    private final Map subscriptionMap = new ConcurrentHashMap();

    public ManagedRegionBroker(BrokerService brokerService,MBeanServer mbeanServer,ObjectName brokerObjectName,
                    TaskRunnerFactory taskRunnerFactory,UsageManager memoryManager,PersistenceAdapter adapter,
                    PolicyMap policyMap) throws IOException{
        super(brokerService,taskRunnerFactory,memoryManager,adapter,policyMap);
        this.mbeanServer=mbeanServer;
        this.brokerObjectName=brokerObjectName;
    }
    
    public void start() throws Exception {
        super.start();
        //build all existing durable subscriptions
        buildExistingSubscriptions();
        
    }

    protected Region createQueueRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory,
                    PersistenceAdapter adapter,PolicyMap policyMap){
        return new ManagedQueueRegion(this,destinationStatistics,memoryManager,taskRunnerFactory,adapter,policyMap);
    }

    protected Region createTempQueueRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory){
        return new ManagedTempQueueRegion(this,destinationStatistics,memoryManager,taskRunnerFactory);
    }

    protected Region createTempTopicRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory){
        return new ManagedTempTopicRegion(this,destinationStatistics,memoryManager,taskRunnerFactory);
    }

    protected Region createTopicRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory,
                    PersistenceAdapter adapter,PolicyMap policyMap){
        return new ManagedTopicRegion(this,destinationStatistics,memoryManager,taskRunnerFactory,adapter,policyMap);
    }

    public void register(ActiveMQDestination destName,Destination destination){
        // Build the object name for the destination
        Hashtable map=new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart(destName.getDestinationTypeAsString()));
        map.put("Destination",JMXSupport.encodeObjectNamePart(destName.getPhysicalName()));
        try{
            ObjectName destObjectName=new ObjectName(brokerObjectName.getDomain(),map);
            DestinationView view;
            if(destination instanceof Queue){
                view=new QueueView((Queue) destination);
            }else{
                view=new TopicView((Topic) destination);
            }
            registerDestination(destObjectName,destName,view);
        }catch(Exception e){
            log.error("Failed to register destination "+destName,e);
        }
    }

    public void unregister(ActiveMQDestination destName){
        // Build the object name for the destination
        Hashtable map=new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart(destName.getDestinationTypeAsString()));
        map.put("Destination",JMXSupport.encodeObjectNamePart(destName.getPhysicalName()));
        try{
            ObjectName destObjectName=new ObjectName(brokerObjectName.getDomain(),map);
            unregisterDestination(destObjectName);
        }catch(Exception e){
            log.error("Failed to unregister "+destName,e);
        }
    }

    public void registerSubscription(ConnectionContext context,Subscription sub){
        SubscriptionKey key = new SubscriptionKey(context.getClientId(),sub.getConsumerInfo().getSubcriptionName());
        Hashtable map=new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart("Subscription"));
        String name = key.toString() + ":" + sub.getConsumerInfo().toString();
        map.put("name",JMXSupport.encodeObjectNamePart(name));
        map.put("active", "true");
        try{
            ObjectName objectName=new ObjectName(brokerObjectName.getDomain(),map);
            SubscriptionView view;
            if(sub.getConsumerInfo().isDurable()){
                view=new DurableSubscriptionView(context.getClientId(),sub);
            }else{
                view=new SubscriptionView(context.getClientId(),sub);
            }
            subscriptionMap.put(sub,objectName);
            registerSubscription(objectName,sub.getConsumerInfo(),key,view);
        }catch(Exception e){
            log.error("Failed to register subscription "+sub,e);
        }
    }

    public void unregisterSubscription(Subscription sub){
        ObjectName name=(ObjectName) subscriptionMap.get(sub);
        if(name!=null){
            try{
                unregisterSubscription(name);
            }catch(Exception e){
                log.error("Failed to unregister subscription "+sub,e);
            }
        }
    }

    protected void registerDestination(ObjectName key,ActiveMQDestination dest,DestinationView view) throws Exception{
        if(dest.isQueue()){
            if(dest.isTemporary()){
                temporaryQueues.put(key,view);
            }else{
                queues.put(key,view);
            }
        }else{
            if(dest.isTemporary()){
                temporaryTopics.put(key,view);
            }else{
                topics.put(key,view);
            }
        }
        mbeanServer.registerMBean(view,key);
    }

    protected void unregisterDestination(ObjectName key) throws Exception{
        topics.remove(key);
        queues.remove(key);
        temporaryQueues.remove(key);
        temporaryTopics.remove(key);
        mbeanServer.unregisterMBean(key);
    }

    protected void registerSubscription(ObjectName key,ConsumerInfo info,SubscriptionKey subscriptionKey,SubscriptionView view) throws Exception{
        ActiveMQDestination dest=info.getDestination();
        if(dest.isQueue()){
            if(dest.isTemporary()){
                temporaryQueueSubscribers.put(key,view);
            }else{
                queueSubscribers.put(key,view);
            }
        }else{
            if(dest.isTemporary()){
                temporaryTopicSubscribers.put(key,view);
            }else{
                if(info.isDurable()){
                    durableTopicSubscribers.put(key,view);
                    //unregister any inactive durable subs
                    try {
                        ObjectName inactiveName = (ObjectName) subscriptionKeys.get(subscriptionKey);
                        if (inactiveName != null){
                            inactiveDurableTopicSubscribers.remove(inactiveName);
                            mbeanServer.unregisterMBean(inactiveName);
                        }
                    }catch(Exception e){
                        log.error("Unable to unregister inactive durable subscriber: " + subscriptionKey,e);
                    }
                }else{
                    topicSubscribers.put(key,view);
                }
            }
        }
        mbeanServer.registerMBean(view,key);
    }

    protected void unregisterSubscription(ObjectName key) throws Exception{
        queueSubscribers.remove(key);
        topicSubscribers.remove(key);
        inactiveDurableTopicSubscribers.remove(key);
        temporaryQueueSubscribers.remove(key);
        temporaryTopicSubscribers.remove(key);
        mbeanServer.unregisterMBean(key);
        DurableSubscriptionView view = (DurableSubscriptionView) durableTopicSubscribers.remove(key);
        if (view != null){
            //need to put this back in the inactive list
            SubscriptionKey subscriptionKey = new SubscriptionKey(view.getClientId(),view.getSubscriptionName());
            SubscriptionInfo info = new SubscriptionInfo();
            info.setClientId(subscriptionKey.getClientId());
            info.setSubcriptionName(subscriptionKey.getSubscriptionName());
            info.setDestination(new ActiveMQTopic(view.getDestinationName()));
            addInactiveSubscription(subscriptionKey, info);
        }
        
       
    }
    
    protected void buildExistingSubscriptions() throws Exception{
        Map subscriptions = new HashMap();
        Set destinations = adaptor.getDestinations();
        if (destinations != null){
            for (Iterator iter = destinations.iterator(); iter.hasNext();){
                ActiveMQDestination dest = (ActiveMQDestination) iter.next();
                if (dest.isTopic()){
                    TopicMessageStore store = adaptor.createTopicMessageStore((ActiveMQTopic) dest);
                    SubscriptionInfo[] infos = store.getAllSubscriptions();
                    if (infos != null){
                        for (int i = 0; i < infos.length; i++) {
                            
                            SubscriptionInfo info = infos[i];
                            log.debug("Restoring durable subscription: "+infos);
                            SubscriptionKey key = new SubscriptionKey(info);
                            subscriptions.put(key,info);
                        }   
                    }
                }
            }
        }
        for (Iterator i = subscriptions.entrySet().iterator();i.hasNext();){
            Map.Entry entry = (Entry) i.next();
            SubscriptionKey key = (SubscriptionKey) entry.getKey();
            SubscriptionInfo info = (SubscriptionInfo) entry.getValue();
            addInactiveSubscription(key, info);
        }
    }
    
    protected void addInactiveSubscription(SubscriptionKey key,SubscriptionInfo info){
        Hashtable map=new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart("Subscription"));
        map.put("name",JMXSupport.encodeObjectNamePart(key.toString()));
        map.put("active", "false");
        try{
            ObjectName objectName=new ObjectName(brokerObjectName.getDomain(),map);
            SubscriptionView view = new InactiveDurableSubscriptionView(key.getClientId(),info);
            mbeanServer.registerMBean(view,objectName);
            inactiveDurableTopicSubscribers.put(objectName,view);
            subscriptionKeys.put(key, objectName);
        }catch(Exception e){
            log.error("Failed to register subscription "+info,e);
        }
    }
    
    protected  ObjectName[] getTopics(){
        Set set = topics.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getQueues(){
        Set set = queues.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getTemporaryTopics(){
        Set set = temporaryTopics.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getTemporaryQueues(){
        Set set = temporaryQueues.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    
    protected ObjectName[] getTopicSubscribers(){
        Set set = topicSubscribers.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getDurableTopicSubscribers(){
        Set set = durableTopicSubscribers.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getQueueSubscribers(){
        Set set = queueSubscribers.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getTemporaryTopicSubscribers(){
        Set set = temporaryTopicSubscribers.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    protected ObjectName[] getTemporaryQueueSubscribers(){
        Set set = temporaryQueueSubscribers.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
    
    protected ObjectName[] getInactiveDurableTopicSubscribers(){
        Set set = inactiveDurableTopicSubscribers.keySet();
        return (ObjectName[])set.toArray(new ObjectName[set.size()]);
    }
}
