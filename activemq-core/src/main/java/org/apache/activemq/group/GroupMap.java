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
package org.apache.activemq.group;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.ConsumerEvent;
import org.apache.activemq.advisory.ConsumerEventSource;
import org.apache.activemq.advisory.ConsumerListener;
import org.apache.activemq.thread.SchedulerTimerTask;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LRUSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * <P>
 * A <CODE>GroupMap</CODE> is used to shared state amongst a distributed
 * group. You can restrict ownership of objects inserted into the map,
 * by allowing only the map that inserted the objects to update or remove them
 * <P>
 * Updates to the group shared map are controlled by a co-ordinator.
 * The co-ordinator is chosen by the member with the lowest lexicographical id .
 * <P>The {@link #selectCordinator(Collection<Member> members)} method may be overridden to 
 * implement a custom mechanism for choosing the co-ordinator
 * are added to the map.
 * <P>
 * @param <K> the key type
 * @param <V> the value type
 *
 */
public class GroupMap<K, V> implements Map<K, V>,Service{
    private static final Log LOG = LogFactory.getLog(GroupMap.class);
    private static final String STATE_TOPIC_PREFIX = GroupMap.class.getName()+".";
    private static final int HEART_BEAT_INTERVAL = 15000; 
    private final Object mapMutex = new Object();
    private Map<EntryKey<K>,EntryValue<V>> localMap;
    private Map<String,Member> members = new ConcurrentHashMap<String,Member>();
    private Map<String,MapRequest> requests = new HashMap<String,MapRequest>();
    private List <MemberChangedListener> membershipListeners = new CopyOnWriteArrayList<MemberChangedListener>();
    private List <MapChangedListener> mapChangedListeners = new CopyOnWriteArrayList<MapChangedListener>();
    private LRUSet<Message>mapUpdateReplies = new LRUSet<Message>();
    private Member local;
    private Member coordinator;
    private String groupName;
    private boolean sharedWrites;
    private Connection connection;
    private Session session;
    private Topic topic;
    private Topic heartBeatTopic;
    private Topic inboxTopic;
    private MessageProducer producer;
    private ConsumerEventSource consumerEvents;
    private AtomicBoolean started = new AtomicBoolean();
    private SchedulerTimerTask heartBeatTask;
    private SchedulerTimerTask checkMembershipTask;
    private Timer heartBeatTimer;
    private int heartBeatInterval = HEART_BEAT_INTERVAL;
    private IdGenerator requestGenerator = new IdGenerator();
    private boolean removeOwnedObjectsOnExit;
    
    /**
     * @param connection
     * @param name
     */
    public GroupMap(Connection connection,String name) {
        this(connection,"default",name);
    }
    
    /**
     * @param connection
     * @param groupName
     * @param name
     */
    public GroupMap(Connection connection,String groupName,String name) {
        this.connection = connection;
        this.local = new Member(name);
        this.coordinator=this.local;
        this.groupName=groupName;
    }
    
    /**
     * Set the local map implementation to be used
     * By default its a HashMap - but you could use a Cache for example
     * @param map
     */
    public void setLocalMap(Map map) {
        synchronized(this.mapMutex) {
            this.localMap=map;
        }
    }
    
    /**
     * Start membership to the group
     * @throws Exception 
     * 
     */
    public void start() throws Exception {
        if(this.started.compareAndSet(false, true)) {
            synchronized(this.mapMutex) {
                if (this.localMap==null) {
                    this.localMap= new HashMap<EntryKey<K>, EntryValue<V>>();
                }
            }
            this.connection.start();
            this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.producer = this.session.createProducer(null);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            this.inboxTopic = this.session.createTemporaryTopic();
            String topicName = STATE_TOPIC_PREFIX+this.groupName;
            this.topic = this.session.createTopic(topicName);
            this.heartBeatTopic = this.session.createTopic(topicName+".heartbeat");
            MessageConsumer privateInbox = this.session.createConsumer(this.inboxTopic);
            privateInbox.setMessageListener(new MessageListener(){
                public void onMessage(Message message) {
                    handleResponses(message);
                }
            });
            ActiveMQMessageConsumer mapChangeConsumer = (ActiveMQMessageConsumer) this.session.createConsumer(this.topic);
            mapChangeConsumer.setMessageListener(new MessageListener(){
                public void onMessage(Message message) {
                   handleMapUpdates(message);
                }
            });
            
            MessageConsumer heartBeatConsumer = this.session.createConsumer(this.heartBeatTopic);
            heartBeatConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    handleHeartbeats(message);
                 }
            });
           
            this.consumerEvents = new ConsumerEventSource(this.connection, this.topic);
            this.consumerEvents.setConsumerListener(new ConsumerListener() {
                public void onConsumerEvent(ConsumerEvent event) {
                    handleConsumerEvents(event);
                }  
            });
            this.consumerEvents.start();
            this.local.setId(mapChangeConsumer.getConsumerId().toString());
            this.local.setInBoxDestination(this.inboxTopic);
            sendHeartBeat();
            this.heartBeatTask = new SchedulerTimerTask (new Runnable() {
                public void run() {
                    sendHeartBeat();
                }
            });
            this.checkMembershipTask = new SchedulerTimerTask(new Runnable() {
                public void run() {
                    checkMembership();
                }
            });
            this.heartBeatTimer = new Timer("Distributed heart beat",true);
            this.heartBeatTimer.scheduleAtFixedRate(this.heartBeatTask, getHeartBeatInterval()/3, getHeartBeatInterval()/2);
            
        }
    }

    /**
     * stop membership to the group
     * @throws Exception 
     */
    public void stop() throws Exception {
        if (this.started.compareAndSet(true, false)) {
            this.checkMembershipTask.cancel();
            this.heartBeatTask.cancel();
            this.heartBeatTimer.purge();
            this.consumerEvents.stop();
            this.session.close();
        }
    }
    
    /**
     * @return the partitionName
     */
    public String getGroupName() {
        return this.groupName;
    }
    
    /**
     * @return the sharedWrites
     */
    public boolean isSharedWrites() {
        return this.sharedWrites;
    }

    /**
     * @param sharedWrites the sharedWrites to set
     */
    public void setSharedWrites(boolean sharedWrites) {
        this.sharedWrites = sharedWrites;
    }
    
    /**
     * @return the heartBeatInterval
     */
    public int getHeartBeatInterval() {
        return this.heartBeatInterval;
    }

    /**
     * @param heartBeatInterval the heartBeatInterval to set
     */
    public void setHeartBeatInterval(int heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
    }
    
    
    /**
     * @param l
     */
    public void addMemberChangedListener(MemberChangedListener l) {
        this.membershipListeners.add(l);
    }
    
    /**
     * @param l
     */
    public void removeMemberChangedListener(MemberChangedListener l) {
        this.membershipListeners.remove(l);
    }
    
    /**
     * @param l
     */
    public void addMapChangedListener(MapChangedListener l) {
        this.mapChangedListeners.add(l);
    }
    
    /**
     * @param l
     */
    public void removeMapChangedListener(MapChangedListener l) {
        this.mapChangedListeners.remove(l);
    }
    
    /**
     * @return the removeOwnedObjectsOnExit
     */
    public boolean isRemoveOwnedObjectsOnExit() {
        return removeOwnedObjectsOnExit;
    }

    /**
     * Sets the policy for owned objects in the group
     * If set to true, when this <code>GroupMap<code> stops,
     * any objects it owns will be removed from the group map
     * @param removeOwnedObjectsOnExit the removeOwnedObjectsOnExit to set
     */
    public void setRemoveOwnedObjectsOnExit(boolean removeOwnedObjectsOnExit) {
        this.removeOwnedObjectsOnExit = removeOwnedObjectsOnExit;
    }
   
    /**
     * clear entries from the Map
     * @throws IllegalStateException 
     */
    public void clear() throws IllegalStateException {
        checkStarted();
        if(this.localMap != null && !this.localMap.isEmpty()) {
            Set<EntryKey<K>> keys = null;
            synchronized(this.mapMutex) {
                keys = new HashSet<EntryKey<K>>(this.localMap.keySet());
                this.localMap.clear();
            }
            
            for(EntryKey<K>key:keys) {
                remove(key);
            }
        }        
    }
    
    
    public boolean containsKey(Object key) {
        EntryKey stateKey = new EntryKey(this.local,key);
        synchronized(this.mapMutex) {
            return this.localMap != null ? this.localMap.containsKey(stateKey):false;
        }
    }

    public boolean containsValue(Object value) {
        EntryValue entryValue = new EntryValue(this.local,value);
        synchronized(this.mapMutex) {
            return this.localMap != null ? this.localMap.containsValue(entryValue):false;
        }
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Map<K,V>result = new HashMap<K,V>();
        synchronized(this.mapMutex) {
            if(this.localMap!=null) {
                for(java.util.Map.Entry<EntryKey<K>,EntryValue<V>>entry:this.localMap.entrySet()) {
                    result.put(entry.getKey().getKey(),entry.getValue().getValue());
                }
            }
        }
        return result.entrySet();
    }

    public V get(Object key) {
       EntryKey<K> stateKey = new EntryKey<K>(this.local,(K) key);
       EntryValue<V> value = null;
       synchronized(this.mapMutex) {
           value = this.localMap != null ?this.localMap.get(stateKey):null;
       }
       return value != null ? value.getValue() : null;
    }

    public boolean isEmpty() {
        synchronized(this.mapMutex) {
            return this.localMap != null ? this.localMap.isEmpty():true;
        }
    }

    public Set<K> keySet() {
        Set <K>result = new HashSet<K>();
        synchronized(this.mapMutex) {
            if(this.localMap!=null) {
                for (EntryKey<K> key:this.localMap.keySet()) {
                    result.add(key.getKey());
                }
            }
        }
        return result;
    }
    /**
     * Puts an value into the map associated with the key
     * @param key 
     * @param value 
     * @return the old value or null
     * @throws IllegalAccessException 
     * @throws IllegalStateException 
     * 
     */
    public V put(K key, V value) throws IllegalAccessException,IllegalStateException{
        return put(key,value,isSharedWrites(),isRemoveOwnedObjectsOnExit());
    }
    
    /**
     * Puts an value into the map associated with the key
     * @param key 
     * @param value 
     * @param sharedWrites 
     * @param removeOnExit 
     * @return the old value or null
     * @throws IllegalAccessException 
     * @throws IllegalStateException 
     * 
     */
    public V put(K key, V value,boolean sharedWrites,boolean removeOnExit) throws IllegalAccessException, IllegalStateException{
        checkStarted();
        EntryKey<K>entryKey = new EntryKey<K>(this.local,key);
        EntryValue<V>stateValue = new EntryValue<V>(this.local,value);
        entryKey.setShare(sharedWrites);
        entryKey.setRemoveOnExit(removeOnExit);
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(entryKey);
        entryMsg.setValue(value);
        entryMsg.setType(EntryMessage.MessageType.INSERT);
        return sendEntryMessage(entryMsg);
    }
    
    /**
     * Add the Map to the distribution
     * @param t
     * @throws IllegalAccessException
     * @throws IllegalStateException
     */
    public void putAll(Map<? extends K, ? extends V> t) throws IllegalAccessException,IllegalStateException {
        putAll(t,isSharedWrites(),isRemoveOwnedObjectsOnExit());
    }

    /**
     * Add the Map to the distribution
     * @param t
     * @param sharedWrites
     * @param removeOnExit
     * @throws IllegalAccessException
     * @throws IllegalStateException
     */
    public void putAll(Map<? extends K, ? extends V> t,boolean sharedWrites,boolean removeOnExit) throws IllegalAccessException,IllegalStateException {
        for(java.util.Map.Entry<? extends K, ? extends V>entry:t.entrySet()) {
            put(entry.getKey(),entry.getValue(),sharedWrites,removeOnExit);
        }        
    }

    /**
     * remove a value from the map associated with the key
     * @param key 
     * @return the Value or null
     * @throws IllegalAccessException 
     * @throws IllegalStateException 
     * 
     */
    public V remove(Object key) throws IllegalAccessException,IllegalStateException{
        EntryKey<K> entryKey = new EntryKey<K>(this.local,(K) key);
        return remove(entryKey);
    }
    
    V remove(EntryKey<K> key) throws IllegalAccessException,IllegalStateException{
        checkStarted();
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(key);
        entryMsg.setType(EntryMessage.MessageType.DELETE);
        return sendEntryMessage(entryMsg);
    }
    
       
    public int size() {
        synchronized(this.mapMutex) {
            return this.localMap != null ? this.localMap.size():0;
        }
    }

    public Collection<V> values() {
        List<V> result = new ArrayList<V>();
        synchronized(this.mapMutex) {
            if(this.localMap!=null) {
                for (EntryValue<V> value:this.localMap.values()) {
                    result.add(value.getValue());
                }
            }
        }
        return result;
    }
    
    /**
     * @return a set of the members 
     */
    public Set<Member> members(){
        Set<Member>result = new HashSet<Member>();
        result.addAll(this.members.values());
        return result;
    }
    
    /**
     * @param key
     * @return true if this is the owner of the key
     */
    public boolean isOwner(K key) {
        EntryKey<K> stateKey = new EntryKey<K>(this.local,key);
        EntryValue<V> entryValue = null;
        synchronized(this.mapMutex) {
            entryValue = this.localMap != null ? this.localMap.get(stateKey):null;
        }
        boolean result = false;
        if (entryValue != null) {
            result = entryValue.getOwner().getId().equals(this.local.getId());
        }
        return result;
    }
    
    /**
     * Get the owner of a key
     * @param key
     * @return the owner - or null if the key doesn't exist
     */
    public Member getOwner(K key) {
        EntryKey<K> stateKey = new EntryKey<K>(this.local,key);
        EntryValue<V> entryValue = null;
        synchronized(this.mapMutex) {
            entryValue = this.localMap != null ? this.localMap.get(stateKey):null;
        }
        return entryValue != null ? entryValue.getOwner():null;
    }
    
    /**
     * @return true if the coordinator for the map
     */
    public boolean isCoordinator() {
        return this.local.equals(this.coordinator);
    }
    
    /**
     * Select a coordinator - by default, its the member with 
     * the lowest lexicographical id 
     * @param members
     * @return
     */
    protected Member selectCordinator(Collection<Member>members) {
        Member result = this.local;
        for (Member member:members) {
            if (result.getId().compareTo(member.getId()) < 0) {
                result = member;
            }
        }
        return result;   
    }
    
    V sendEntryMessage(EntryMessage entry) {
        Object result = null;
        MapRequest request = new MapRequest();
        String id = this.requestGenerator.generateId();
        synchronized(this.requests) {
            this.requests.put(id,request);
        }
        try {
            ObjectMessage objMsg = this.session.createObjectMessage(entry);
            objMsg.setJMSReplyTo(this.inboxTopic);
            objMsg.setJMSCorrelationID(id);
            this.producer.send(this.topic, objMsg);
            result = request.get(getHeartBeatInterval()*2);
        }catch(JMSException e) {
            if(this.started.get()) {
                LOG.error("Failed to send EntryMessage " + entry,e);
            }
        }
        if (result instanceof IllegalAccessException) {
            throw (IllegalAccessException)result;
        }
        return (V) result;
    }
    
    void handleResponses(Message message) {
        if (message instanceof ObjectMessage) {
            ObjectMessage objMsg = (ObjectMessage) message;
            try {
                Object payload = objMsg.getObject();
                if (payload instanceof Member) {
                    handleHeartbeats((Member)payload);
                } else if(payload instanceof EntryMessage) {
                    EntryMessage entryMsg = (EntryMessage) payload;
                    EntryKey<K>key=entryMsg.getKey();
                    EntryValue<V> value = new EntryValue<V>(key.getOwner(),(V) entryMsg.getValue());
                    
                    if(this.localMap !=null) {
                        boolean fireUpdate = false;
                        synchronized(this.mapMutex) {
                            if(!this.localMap.containsKey(key)) {
                                this.localMap.put(key,value);
                                fireUpdate=true;
                            }
                        }
                        if(fireUpdate) {
                            fireMapChanged(key.getOwner(), key.getKey(), null, value.getValue());
                        }
                    }
                   
                    
                }else {
                    String id = objMsg.getJMSCorrelationID();
                    MapRequest result = null;
                    synchronized (this.requests) {
                        result = this.requests.remove(id);
                    }
                    if (result != null) {
                        result.put(objMsg.getObject());
                    }
                }
            } catch (JMSException e) {
                LOG.warn("Failed to process reply: " + message, e);
            }
        }
    }
    
    void handleMapUpdates(Message message) {
        Object reply = null;
        if (message instanceof ObjectMessage) {
            try {
                ObjectMessage objMsg = (ObjectMessage) message;
                EntryMessage entryMsg = (EntryMessage) objMsg.getObject();
                EntryKey<K> key = entryMsg.getKey();
                EntryValue<V> value = new EntryValue<V>(key.getOwner(),(V) entryMsg.getValue());
                boolean containsKey=false;
                boolean mapExists = false;
                synchronized(this.mapMutex) {
                    mapExists = this.localMap!=null;
                    if(mapExists) {
                        containsKey=this.localMap.containsKey(key);
                    }
                }
                if(mapExists) {
                    if (containsKey) {
                        Member owner = getOwner((K) key.getKey());
                        if (owner.equals(key.getOwner()) && !key.isShare()) {
                            EntryValue<V> old = null;
                            if (entryMsg.getType().equals(EntryMessage.MessageType.INSERT)) {
                                synchronized(this.mapMutex) {
                                    old = this.localMap.put(key, value);     
                                }
                            }else {
                                synchronized(this.mapMutex) {
                                    old = this.localMap.remove(key);
                                }
                            }
                            fireMapChanged(owner, key.getKey(), old.getValue(), value.getValue());
                        }else {
                            reply = new IllegalAccessException("Owned by "+ owner);
                        }
                    }else {
                        if (entryMsg.getType().equals(EntryMessage.MessageType.INSERT)) {
                            synchronized(this.mapMutex) {
                                this.localMap.put(key, value);
                            }
                            fireMapChanged(key.getOwner(), key.getKey(), null, value.getValue());
                        }
                    }
                }
            } catch (JMSException e) {
                LOG.warn("Failed to process map update",e);
                reply = e;
            }
           
            try {
                Destination replyTo = message.getJMSReplyTo();
                String correlationId = message.getJMSCorrelationID();
                ObjectMessage replyMsg = this.session
                        .createObjectMessage((Serializable) reply);
                replyMsg.setJMSCorrelationID(correlationId);
                // reuse timestamp - this will be cleared by the producer on
                // send
                replyMsg.setJMSTimestamp(System.currentTimeMillis());
                if (isCoordinator()) {
                    this.producer.send(replyTo, replyMsg);
                }else {
                    synchronized(mapUpdateReplies) {
                        this.mapUpdateReplies.add(replyMsg);
                    }
                }
            } catch (JMSException e) {
                if(this.started.get()) {
                    LOG.error("Failed to send response to a map update ", e);
                }
            }
            
            
        }else {
            LOG.warn("Unexpected map update message " + message);
        }
    }
    
    void handleHeartbeats(Message message) {
        try {
            if (message instanceof ObjectMessage) {
                ObjectMessage objMsg = (ObjectMessage) message;
                Member member = (Member) objMsg.getObject();
                handleHeartbeats(member);
            } else {
                LOG.warn("Unexpected message: " + message);
            }
        } catch (JMSException e) {
            LOG.warn("Failed to handle heart beat", e);
        }
    }

    void handleHeartbeats(Member member) {
        member.setTimeStamp(System.currentTimeMillis());
        if (this.members.put(member.getId(), member) == null) {
            fireMemberStarted(member);
            if (!member.equals(this.local)) {
                //send the new member our details
                sendHeartBeat(member.getInBoxDestination());
                if(isCoordinator()) {
                    updateNewMemberMap(member);
                }
            }
        }
    }
    
    void handleConsumerEvents(ConsumerEvent event) {
        if (!event.isStarted()) {
            Member member = this.members.remove(event.getConsumerId().toString());
            if(member!=null) {
                fireMemberStopped(member);
                doElection();
            }
        }
    }
    
    void checkMembership() {
        if (this.started.get()) {
            long checkTime = System.currentTimeMillis()-getHeartBeatInterval();
            boolean doElection = false;
            for (Member member : this.members.values()) {
                if (member.getTimeStamp()<checkTime) {
                    LOG.info("Member timestamp expired " + member);
                    this.members.remove(member.getId());
                    fireMemberStopped(member);
                    doElection=true;
                    
                }
            }
            if (doElection) {
                doElection();
            }
        }
        //clear down cached reply messages
        long checkTime = System.currentTimeMillis()-(getHeartBeatInterval()*2);
        List<Message> tmpList = new ArrayList<Message>();
        synchronized(this.mapUpdateReplies) {
            try {
                for(Message msg:this.mapUpdateReplies) {
                    if (msg.getJMSTimestamp() < checkTime) {
                        tmpList.add(msg);
                    }
                }
                for(Message msg:tmpList) {
                    this.mapUpdateReplies.remove(msg);
                }
            }catch(JMSException e) {
                LOG.warn("Failed to clear down mapUpdateReplies",e);
            }
        }
    }
    
    void sendHeartBeat() {
        sendHeartBeat(this.heartBeatTopic);
    }

    void sendHeartBeat(Destination destination) {
        if (this.started.get()) {
            try {
                ObjectMessage msg = this.session
                        .createObjectMessage(this.local);
                this.producer.send(destination, msg);
            } catch (Throwable e) {
                if(this.started.get()) {
                    LOG.warn("Failed to send heart beat", e);
                }
            }
        }
    }
    
    void updateNewMemberMap(Member member) {
        List<Map.Entry<EntryKey<K>, EntryValue<V>>> list = new ArrayList<Map.Entry<EntryKey<K>, EntryValue<V>>>();
        synchronized (this.mapMutex) {
            if (this.localMap != null) {
                for (Map.Entry<EntryKey<K>, EntryValue<V>> entry : this.localMap
                        .entrySet()) {
                    list.add(entry);
                }
            }
        }
        try {
            for (Map.Entry<EntryKey<K>, EntryValue<V>> entry : list) {
                EntryMessage entryMsg = new EntryMessage();
                entryMsg.setKey(entry.getKey());
                entryMsg.setValue(entry.getValue().getValue());
                entryMsg.setType(EntryMessage.MessageType.INSERT);
                ObjectMessage objMsg = this.session
                        .createObjectMessage(entryMsg);
                if(!member.equals(entry.getKey().getOwner())) {
                    this.producer.send(member.getInBoxDestination(), objMsg);
                }
            }
        } catch (JMSException e) {
            if (started.get()) {
                LOG.warn("Failed to update new member ", e);
            }
        }
    }
    
    void fireMemberStarted(Member member) {
        LOG.info(this.local.getName()+" Member started " + member);
        for (MemberChangedListener l : this.membershipListeners) {
            l.memberStarted(member);
        }
    }
    
    void fireMemberStopped(Member member) {
        LOG.info(this.local.getName()+" Member stopped " + member);
        for (MemberChangedListener l : this.membershipListeners) {
            l.memberStopped(member);
        }
        //remove all entries owned by the stopped member
        List<EntryKey<K>>tmpList = new ArrayList<EntryKey<K>>();
        boolean mapExists = false;
        synchronized(this.mapMutex) {
            mapExists=this.localMap!=null;
            if(mapExists) {
                for (EntryKey<K> entryKey:this.localMap.keySet()) {
                    if (entryKey.getOwner().equals(member)) {
                        if (entryKey.isRemoveOnExit()) {
                            tmpList.add(entryKey);
                        }
                    }
                }
            }
        }
        if(mapExists) {
            for (EntryKey<K> entryKey:tmpList) {
                EntryValue<V> value = null;
                synchronized(this.mapMutex) {
                    value = this.localMap.remove(entryKey);
                }
                fireMapChanged(member, entryKey.getKey(), value.getValue(), null);
            }
        }
    }
    
    void fireMapChanged(Member owner,Object key, Object oldValue, Object newValue) {
        for (MapChangedListener l:this.mapChangedListeners) {
            l.mapChanged(owner, key, oldValue, newValue);
        }
    }
    
    void doElection() {
        this.coordinator=selectCordinator(this.members.values());
        if (isCoordinator() && this.started.get()) {
            //send any inflight requests
            List<Message>list = new ArrayList<Message>();
            synchronized(this.mapUpdateReplies) {
                list.addAll(this.mapUpdateReplies);
                this.mapUpdateReplies.clear();
            }
            try {
            for(Message msg:list) {
                if (this.started.get()) {
                    this.producer.send(msg.getJMSReplyTo(), msg);
                }
            }
            }catch(JMSException e) {
                if(this.started.get()) {
                    LOG.error("Failed to resend replies",e);
                }
            }
        }
    } 
    
    void checkStarted() throws IllegalStateException{
        if (!started.get()) {
            throw new IllegalStateException("GroupMap " + this.local.getName() + " not started");
        }
    }   
}
