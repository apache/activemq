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
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.JMXSupport;
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
    private final Map temporaryQueueSubscribers=new ConcurrentHashMap();
    private final Map temporaryTopicSubscribers=new ConcurrentHashMap();

    public ManagedRegionBroker(BrokerService brokerService,MBeanServer mbeanServer,ObjectName brokerObjectName,
                    TaskRunnerFactory taskRunnerFactory,UsageManager memoryManager,PersistenceAdapter adapter,
                    PolicyMap policyMap) throws IOException{
        super(brokerService,taskRunnerFactory,memoryManager,adapter,policyMap);
        this.mbeanServer=mbeanServer;
        this.brokerObjectName=brokerObjectName;
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

    public void registerSubscription(Subscription sub){
        Hashtable map=new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart("Subscription"));
        map.put("name",JMXSupport.encodeObjectNamePart(sub.getConsumerInfo().toString()));
        try{
            ObjectName objectName=new ObjectName(brokerObjectName.getDomain(),map);
            SubscriptionView view;
            if(sub.getConsumerInfo().isDurable()){
                view=new DurableSubscriptionView(sub);
            }else{
                view=new SubscriptionView(sub);
            }
            registerSubscription(objectName,sub.getConsumerInfo(),view);
        }catch(Exception e){
            log.error("Failed to register subscription "+sub,e);
        }
    }

    public void unregisterSubscription(Subscription sub){
        Hashtable map=new Hashtable(brokerObjectName.getKeyPropertyList());
        map.put("Type",JMXSupport.encodeObjectNamePart("Subscription"));
        map.put("name",JMXSupport.encodeObjectNamePart(sub.getConsumerInfo().toString()));
        try{
            ObjectName objectName=new ObjectName(brokerObjectName.getDomain(),map);
            unregisterSubscription(objectName);
        }catch(Exception e){
            log.error("Failed to unregister subscription "+sub,e);
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

    protected void registerSubscription(ObjectName key,ConsumerInfo info,SubscriptionView view) throws Exception{
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
        durableTopicSubscribers.remove(key);
        temporaryQueueSubscribers.remove(key);
        temporaryTopicSubscribers.remove(key);
        mbeanServer.unregisterMBean(key);
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
}
