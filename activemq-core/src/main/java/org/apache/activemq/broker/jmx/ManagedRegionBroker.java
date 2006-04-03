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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
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
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArraySet;
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
    private final Map subscriptionKeys=new ConcurrentHashMap();
    private final Map subscriptionMap=new ConcurrentHashMap();
    private final Set registeredMBeans=new CopyOnWriteArraySet();
    /* This is the first broker in the broker interceptor chain. */
    private Broker contextBroker;

    public ManagedRegionBroker(BrokerService brokerService,MBeanServer mbeanServer,ObjectName brokerObjectName,
                    TaskRunnerFactory taskRunnerFactory,UsageManager memoryManager,PersistenceAdapter adapter)
                    throws IOException{
        super(brokerService,taskRunnerFactory,memoryManager,adapter);
        this.mbeanServer=mbeanServer;
        this.brokerObjectName=brokerObjectName;
    }

    public void start() throws Exception{
        super.start();
        // build all existing durable subscriptions
        buildExistingSubscriptions();
    }

    protected void doStop(ServiceStopper stopper){
        super.doStop(stopper);
        // lets remove any mbeans not yet removed
        for(Iterator iter=registeredMBeans.iterator();iter.hasNext();){
            ObjectName name=(ObjectName) iter.next();
            try{
                mbeanServer.unregisterMBean(name);
            }catch(InstanceNotFoundException e){
                log.warn("The MBean: "+name+" is no longer registered with JMX");
            }catch(Exception e){
                stopper.onException(this,e);
            }
        }
        registeredMBeans.clear();
    }

    protected Region createQueueRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory,
                    PersistenceAdapter adapter){
        return new ManagedQueueRegion(this,destinationStatistics,memoryManager,taskRunnerFactory,adapter);
    }

    protected Region createTempQueueRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory){
        return new ManagedTempQueueRegion(this,destinationStatistics,memoryManager,taskRunnerFactory);
    }

    protected Region createTempTopicRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory){
        return new ManagedTempTopicRegion(this,destinationStatistics,memoryManager,taskRunnerFactory);
    }

    protected Region createTopicRegion(UsageManager memoryManager,TaskRunnerFactory taskRunnerFactory,
                    PersistenceAdapter adapter){
        return new ManagedTopicRegion(this,destinationStatistics,memoryManager,taskRunnerFactory,adapter);
    }

    public void register(ActiveMQDestination destName,Destination destination){
        try{
            ObjectName objectName=createObjectName(destName);
            DestinationView view;
            if(destination instanceof Queue){
                view=new QueueView(this,(Queue) destination);
            }else{
                view=new TopicView(this,(Topic) destination);
            }
            registerDestination(objectName,destName,view);
        }catch(Exception e){
            log.error("Failed to register destination "+destName,e);
        }
    }

    public void unregister(ActiveMQDestination destName){
        try{
            ObjectName objectName=createObjectName(destName);
            unregisterDestination(objectName);
        }catch(Exception e){
            log.error("Failed to unregister "+destName,e);
        }
    }

    public ObjectName registerSubscription(ConnectionContext context,Subscription sub){
        Hashtable map=brokerObjectName.getKeyPropertyList();
        String name="";
        SubscriptionKey key=new SubscriptionKey(context.getClientId(),sub.getConsumerInfo().getSubcriptionName());
        if(sub.getConsumerInfo().isDurable()){
            name=key.toString();
        }else{
            name=sub.getConsumerInfo().getConsumerId().toString();
        }
        try{
            ObjectName objectName=new ObjectName(brokerObjectName.getDomain()+":"+"BrokerName="+map.get("BrokerName")
                            +","+"Type=Subscription,"+"active=true,"+"name="+JMXSupport.encodeObjectNamePart(name)+"");
            SubscriptionView view;
            if(sub.getConsumerInfo().isDurable()){
                view=new DurableSubscriptionView(this,context.getClientId(),sub);
            }else{
                if(sub instanceof TopicSubscription){
                    view=new TopicSubscriptionView(context.getClientId(),(TopicSubscription) sub);
                }else{
                    view=new SubscriptionView(context.getClientId(),sub);
                }
            }
            registerSubscription(objectName,sub.getConsumerInfo(),key,view);
            subscriptionMap.put(sub,objectName);
            return objectName;
        }catch(Exception e){
            log.error("Failed to register subscription "+sub,e);
            return null;
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
        registeredMBeans.add(key);
        mbeanServer.registerMBean(view,key);
    }

    protected void unregisterDestination(ObjectName key) throws Exception{
        topics.remove(key);
        queues.remove(key);
        temporaryQueues.remove(key);
        temporaryTopics.remove(key);
        if(registeredMBeans.remove(key)){
            mbeanServer.unregisterMBean(key);
        }
    }

    protected void registerSubscription(ObjectName key,ConsumerInfo info,SubscriptionKey subscriptionKey,
                    SubscriptionView view) throws Exception{
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
                    // unregister any inactive durable subs
                    try{
                        ObjectName inactiveName=(ObjectName) subscriptionKeys.get(subscriptionKey);
                        if(inactiveName!=null){
                            inactiveDurableTopicSubscribers.remove(inactiveName);
                            registeredMBeans.remove(inactiveName);
                            mbeanServer.unregisterMBean(inactiveName);
                        }
                    }catch(Exception e){
                        log.error("Unable to unregister inactive durable subscriber: "+subscriptionKey,e);
                    }
                }else{
                    topicSubscribers.put(key,view);
                }
            }
        }
        registeredMBeans.add(key);
        mbeanServer.registerMBean(view,key);
    }

    protected void unregisterSubscription(ObjectName key) throws Exception{
        queueSubscribers.remove(key);
        topicSubscribers.remove(key);
        inactiveDurableTopicSubscribers.remove(key);
        temporaryQueueSubscribers.remove(key);
        temporaryTopicSubscribers.remove(key);
        if(registeredMBeans.remove(key)){
            mbeanServer.unregisterMBean(key);
        }
        DurableSubscriptionView view=(DurableSubscriptionView) durableTopicSubscribers.remove(key);
        if(view!=null){
            // need to put this back in the inactive list
            SubscriptionKey subscriptionKey=new SubscriptionKey(view.getClientId(),view.getSubscriptionName());
            SubscriptionInfo info=new SubscriptionInfo();
            info.setClientId(subscriptionKey.getClientId());
            info.setSubcriptionName(subscriptionKey.getSubscriptionName());
            info.setDestination(new ActiveMQTopic(view.getDestinationName()));
            addInactiveSubscription(subscriptionKey,info);
        }
    }

    protected void buildExistingSubscriptions() throws Exception{
        Map subscriptions=new HashMap();
        Set destinations=adaptor.getDestinations();
        if(destinations!=null){
            for(Iterator iter=destinations.iterator();iter.hasNext();){
                ActiveMQDestination dest=(ActiveMQDestination) iter.next();
                if(dest.isTopic()){
                    TopicMessageStore store=adaptor.createTopicMessageStore((ActiveMQTopic) dest);
                    SubscriptionInfo[] infos=store.getAllSubscriptions();
                    if(infos!=null){
                        for(int i=0;i<infos.length;i++){
                            SubscriptionInfo info=infos[i];
                            log.debug("Restoring durable subscription: "+infos);
                            SubscriptionKey key=new SubscriptionKey(info);
                            subscriptions.put(key,info);
                        }
                    }
                }
            }
        }
        for(Iterator i=subscriptions.entrySet().iterator();i.hasNext();){
            Map.Entry entry=(Entry) i.next();
            SubscriptionKey key=(SubscriptionKey) entry.getKey();
            SubscriptionInfo info=(SubscriptionInfo) entry.getValue();
            addInactiveSubscription(key,info);
        }
    }

    protected void addInactiveSubscription(SubscriptionKey key,SubscriptionInfo info){
        Hashtable map=brokerObjectName.getKeyPropertyList();
        try{
            ObjectName objectName=new ObjectName(brokerObjectName.getDomain()+":"+"BrokerName="+map.get("BrokerName")
                            +","+"Type=Subscription,"+"active=false,"+"name="
                            +JMXSupport.encodeObjectNamePart(key.toString())+"");
            SubscriptionView view=new InactiveDurableSubscriptionView(this,key.getClientId(),info);
            registeredMBeans.add(objectName);
            mbeanServer.registerMBean(view,objectName);
            inactiveDurableTopicSubscribers.put(objectName,view);
            subscriptionKeys.put(key,objectName);
        }catch(Exception e){
            log.error("Failed to register subscription "+info,e);
        }
    }

    public CompositeData[] browse(SubscriptionView view) throws OpenDataException{
        List messages=getSubscriberMessages(view);
        CompositeData c[]=new CompositeData[messages.size()];
        for(int i=0;i<c.length;i++){
            try{
                c[i]=OpenTypeSupport.convert((Message) messages.get(i));
            }catch(Throwable e){
                e.printStackTrace();
            }
        }
        return c;
    }

    public TabularData browseAsTable(SubscriptionView view) throws OpenDataException{
        OpenTypeFactory factory=OpenTypeSupport.getFactory(ActiveMQMessage.class);
        List messages=getSubscriberMessages(view);
        CompositeType ct=factory.getCompositeType();
        TabularType tt=new TabularType("MessageList","MessageList",ct,new String[] { "JMSMessageID" });
        TabularDataSupport rc=new TabularDataSupport(tt);
        for(int i=0;i<messages.size();i++){
            rc.put(new CompositeDataSupport(ct,factory.getFields(messages.get(i))));
        }
        return rc;
    }

    protected List getSubscriberMessages(SubscriptionView view){
        final List result=new ArrayList();
        try{
            ActiveMQTopic topic=new ActiveMQTopic(view.getDestinationName());
            TopicMessageStore store=adaptor.createTopicMessageStore(topic);
            store.recover(new MessageRecoveryListener(){
                public void recoverMessage(Message message) throws Exception{
                    result.add(message);
                }

                public void recoverMessageReference(String messageReference) throws Exception{}

                public void finished(){}
            });
        }catch(Throwable e){
            log.error("Failed to browse messages for Subscription "+view,e);
        }
        return result;
    }

    protected ObjectName[] getTopics(){
        Set set=topics.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getQueues(){
        Set set=queues.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryTopics(){
        Set set=temporaryTopics.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryQueues(){
        Set set=temporaryQueues.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTopicSubscribers(){
        Set set=topicSubscribers.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getDurableTopicSubscribers(){
        Set set=durableTopicSubscribers.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getQueueSubscribers(){
        Set set=queueSubscribers.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryTopicSubscribers(){
        Set set=temporaryTopicSubscribers.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getTemporaryQueueSubscribers(){
        Set set=temporaryQueueSubscribers.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    protected ObjectName[] getInactiveDurableTopicSubscribers(){
        Set set=inactiveDurableTopicSubscribers.keySet();
        return (ObjectName[]) set.toArray(new ObjectName[set.size()]);
    }

    public Broker getContextBroker(){
        return contextBroker;
    }

    public void setContextBroker(Broker contextBroker){
        this.contextBroker=contextBroker;
    }

    protected ObjectName createObjectName(ActiveMQDestination destName) throws MalformedObjectNameException{
        // Build the object name for the destination
        Hashtable map=brokerObjectName.getKeyPropertyList();
        ObjectName objectName=new ObjectName(brokerObjectName.getDomain()+":"+"BrokerName="+map.get("BrokerName")+","
                        +"Type="+JMXSupport.encodeObjectNamePart(destName.getDestinationTypeAsString())+","
                        +"Destination="+JMXSupport.encodeObjectNamePart(destName.getPhysicalName()));
        return objectName;
    }
}
