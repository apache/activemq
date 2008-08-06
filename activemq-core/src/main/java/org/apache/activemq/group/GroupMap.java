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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <P>
 * A <CODE>GroupMap</CODE> is a Map implementation that is used to shared state 
 * amongst a distributed group of other <CODE>GroupMap</CODE> instances. 
 * Membership of a group is handled automatically using discovery.
 * <P>
 * The underlying transport is JMS and there are some optimizations that occur
 * for membership if used with ActiveMQ - but <CODE>GroupMap</CODE> can be used
 * with any JMS implementation.
 * 
 * <P>
 * Updates to the group shared map are controlled by a coordinator. The
 * coordinator is elected by the member with the lowest lexicographical id - 
 * based on the bully algorithm [Silberschatz et al. 1993]
 * <P>
 * The {@link #selectCordinator(Collection<Member> members)} method may be
 * overridden to implement a custom mechanism for choosing  how the coordinator
 * is elected for the map.
 * <P>
 * New <CODE>GroupMap</CODE> instances have their state updated by the coordinator,
 * and coordinator failure is handled automatically within the group.
 * <P>
 * All updates are totally ordered through the coordinator, whilst read operations 
 * happen locally. 
 * <P>
 * A <CODE>GroupMap</CODE>supports the concept of owner only updates(write locks), 
 * shared updates, entry expiration times and removal on owner exit - 
 * all of which are optional.
 * 
 * <P>
 * 
 * @param <K> the key type
 * @param <V> the value type
 * 
 */
public class GroupMap<K, V> implements Map<K, V>, Service {
    /**
     * default interval within which to detect a member failure
     */
    public static final long DEFAULT_HEART_BEAT_INTERVAL = 2000;
    private static final long EXPIRATION_SWEEP_INTERVAL = 1000;
    private static final Log LOG = LogFactory.getLog(GroupMap.class);
    private static final String STATE_TOPIC_PREFIX = GroupMap.class.getName()
            + ".";
    private final Object mapMutex = new Object();
    private Map<EntryKey<K>, EntryValue<V>> localMap;
    private Map<String, Member> members = new ConcurrentHashMap<String, Member>();
    private Map<String, MapRequest> requests = new HashMap<String, MapRequest>();
    private List<MemberChangedListener> membershipListeners = new CopyOnWriteArrayList<MemberChangedListener>();
    private List<MapChangedListener> mapChangedListeners = new CopyOnWriteArrayList<MapChangedListener>();
    Member local;
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
    private SchedulerTimerTask expirationTask;
    private Timer timer;
    private long heartBeatInterval = DEFAULT_HEART_BEAT_INTERVAL;
    private IdGenerator idGenerator = new IdGenerator();
    private boolean removeOwnedObjectsOnExit;
    private int timeToLive;
    private int minimumGroupSize = 1;
    private final AtomicBoolean electionFinished = new AtomicBoolean(true);
    private ExecutorService executor;
    private final Object memberMutex = new Object();

    /**
     * @param connection
     * @param name
     */
    public GroupMap(Connection connection, String name) {
        this(connection, "default", name);
    }

    /**
     * @param connection
     * @param groupName
     * @param name
     */
    public GroupMap(Connection connection, String groupName, String name) {
        this.connection = connection;
        this.local = new Member(name);
        this.coordinator = this.local;
        this.groupName = groupName;
    }

    /**
     * Set the local map implementation to be used By default its a HashMap -
     * but you could use a Cache for example
     * 
     * @param map
     */
    public void setLocalMap(Map map) {
        synchronized (this.mapMutex) {
            this.localMap = map;
        }
    }

    /**
     * Start membership to the group
     * 
     * @throws Exception
     * 
     */
    public void start() throws Exception {
        if (this.started.compareAndSet(false, true)) {
            synchronized (this.mapMutex) {
                if (this.localMap == null) {
                    this.localMap = new HashMap<EntryKey<K>, EntryValue<V>>();
                }
            }
            this.connection.start();
            this.session = this.connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            this.producer = this.session.createProducer(null);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            this.inboxTopic = this.session.createTemporaryTopic();
            String topicName = STATE_TOPIC_PREFIX + this.groupName;
            this.topic = this.session.createTopic(topicName);
            this.heartBeatTopic = this.session.createTopic(topicName
                    + ".heartbeat");
            MessageConsumer privateInbox = this.session
                    .createConsumer(this.inboxTopic);
            privateInbox.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    processMessage(message);
                }
            });
            MessageConsumer mapChangeConsumer = this.session
                    .createConsumer(this.topic);
            mapChangeConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    processMessage(message);
                }
            });
            MessageConsumer heartBeatConsumer = this.session
                    .createConsumer(this.heartBeatTopic);
            heartBeatConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    handleHeartbeats(message);
                }
            });
            this.consumerEvents = new ConsumerEventSource(this.connection,
                    this.topic);
            this.consumerEvents.setConsumerListener(new ConsumerListener() {
                public void onConsumerEvent(ConsumerEvent event) {
                    handleConsumerEvents(event);
                }
            });
            this.consumerEvents.start();
            String memberId = null;
            if (mapChangeConsumer instanceof ActiveMQMessageConsumer) {
                memberId = ((ActiveMQMessageConsumer) mapChangeConsumer)
                        .getConsumerId().toString();
            } else {
                memberId = this.idGenerator.generateId();
            }
            this.local.setId(memberId);
            this.local.setInBoxDestination(this.inboxTopic);
            this.executor = Executors
                    .newSingleThreadExecutor(new ThreadFactory() {
                        public Thread newThread(Runnable runnable) {
                            Thread thread = new Thread(runnable, "Election{"
                                    + GroupMap.this.local + "}");
                            thread.setDaemon(true);
                            thread.setPriority(Thread.NORM_PRIORITY);
                            return thread;
                        }
                    });
            sendHeartBeat();
            this.heartBeatTask = new SchedulerTimerTask(new Runnable() {
                public void run() {
                    sendHeartBeat();
                }
            });
            this.checkMembershipTask = new SchedulerTimerTask(new Runnable() {
                public void run() {
                    checkMembership();
                }
            });
            
            this.expirationTask = new SchedulerTimerTask(new Runnable() {
                public void run() {
                    expirationSweep();
                }
            });
            this.timer = new Timer("Distributed heart beat", true);
            this.timer.scheduleAtFixedRate(this.heartBeatTask,
                    getHeartBeatInterval() / 3, getHeartBeatInterval() / 2);
            this.timer.scheduleAtFixedRate(this.checkMembershipTask,
                    getHeartBeatInterval(), getHeartBeatInterval());
            this.timer.scheduleAtFixedRate(this.expirationTask,
                    EXPIRATION_SWEEP_INTERVAL, EXPIRATION_SWEEP_INTERVAL);
            // await for members to join
            long timeout = this.heartBeatInterval * this.minimumGroupSize;
            long deadline = System.currentTimeMillis() + timeout;
            while (this.members.size() < this.minimumGroupSize && timeout > 0) {
                synchronized (this.memberMutex) {
                    this.memberMutex.wait(timeout);
                }
                timeout = Math.max(deadline - System.currentTimeMillis(), 0);
            }
        }
    }

    /**
     * stop membership to the group
     * 
     * @throws Exception
     */
    public void stop() {
        if (this.started.compareAndSet(true, false)) {
            this.expirationTask.cancel();
            this.checkMembershipTask.cancel();
            this.heartBeatTask.cancel();
            this.expirationTask.cancel();
            this.timer.purge();
            if (this.executor != null) {
                this.executor.shutdownNow();
            }
            try {
                this.consumerEvents.stop();
                this.session.close();
            } catch (Exception e) {
                LOG.debug("Caught exception stopping", e);
            }
        }
    }

    /**
     * @return true if there is elections have finished
     */
    public boolean isElectionFinished() {
        return this.electionFinished.get();
    }

    /**
     * @return the partitionName
     */
    public String getGroupName() {
        return this.groupName;
    }

    /**
     * @return the name ofthis map
     */
    public String getName() {
        return this.local.getName();
    }

    /**
     * @return the sharedWrites
     */
    public boolean isSharedWrites() {
        return this.sharedWrites;
    }

    /**
     * @param sharedWrites
     *            the sharedWrites to set
     */
    public void setSharedWrites(boolean sharedWrites) {
        this.sharedWrites = sharedWrites;
    }

    /**
     * @return the heartBeatInterval
     */
    public long getHeartBeatInterval() {
        return this.heartBeatInterval;
    }

    /**
     * @param heartBeatInterval
     *            the heartBeatInterval to set
     */
    public void setHeartBeatInterval(long heartBeatInterval) {
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
     * @return the timeToLive
     */
    public int getTimeToLive() {
        return this.timeToLive;
    }

    /**
     * @param timeToLive
     *            the timeToLive to set
     */
    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }

    /**
     * @return the removeOwnedObjectsOnExit
     */
    public boolean isRemoveOwnedObjectsOnExit() {
        return this.removeOwnedObjectsOnExit;
    }

    /**
     * Sets the policy for owned objects in the group If set to true, when this
     * <code>GroupMap<code> stops,
     * any objects it owns will be removed from the group map
     * @param removeOwnedObjectsOnExit the removeOwnedObjectsOnExit to set
     */
    public void setRemoveOwnedObjectsOnExit(boolean removeOwnedObjectsOnExit) {
        this.removeOwnedObjectsOnExit = removeOwnedObjectsOnExit;
    }

    /**
     * @return the minimumGroupSize
     */
    public int getMinimumGroupSize() {
        return this.minimumGroupSize;
    }

    /**
     * @param minimumGroupSize
     *            the minimumGroupSize to set
     */
    public void setMinimumGroupSize(int minimumGroupSize) {
        this.minimumGroupSize = minimumGroupSize;
    }

    /**
     * clear entries from the Map
     * 
     * @throws IllegalStateException
     */
    public void clear() throws IllegalStateException {
        checkStatus();
        if (this.localMap != null && !this.localMap.isEmpty()) {
            Set<EntryKey<K>> keys = null;
            synchronized (this.mapMutex) {
                keys = new HashSet<EntryKey<K>>(this.localMap.keySet());
            }
            for (EntryKey<K> key : keys) {
                remove(key);
            }
        }
        this.localMap.clear();
    }

    public boolean containsKey(Object key) {
        EntryKey stateKey = new EntryKey(this.local, key);
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap.containsKey(stateKey)
                    : false;
        }
    }

    public boolean containsValue(Object value) {
        EntryValue entryValue = new EntryValue(this.local, value);
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap
                    .containsValue(entryValue) : false;
        }
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Map<K, V> result = new HashMap<K, V>();
        synchronized (this.mapMutex) {
            if (this.localMap != null) {
                for (java.util.Map.Entry<EntryKey<K>, EntryValue<V>> entry : this.localMap
                        .entrySet()) {
                    result.put(entry.getKey().getKey(), entry.getValue()
                            .getValue());
                }
            }
        }
        return result.entrySet();
    }

    public V get(Object key) {
        EntryKey<K> stateKey = new EntryKey<K>(this.local, (K) key);
        EntryValue<V> value = null;
        synchronized (this.mapMutex) {
            value = this.localMap != null ? this.localMap.get(stateKey) : null;
        }
        return value != null ? value.getValue() : null;
    }

    public boolean isEmpty() {
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap.isEmpty() : true;
        }
    }

    public Set<K> keySet() {
        Set<K> result = new HashSet<K>();
        synchronized (this.mapMutex) {
            if (this.localMap != null) {
                for (EntryKey<K> key : this.localMap.keySet()) {
                    result.add(key.getKey());
                }
            }
        }
        return result;
    }

    /**
     * Puts an value into the map associated with the key
     * 
     * @param key
     * @param value
     * @return the old value or null
     * @throws GroupMapUpdateException
     * @throws IllegalStateException
     * 
     */
    public V put(K key, V value) throws GroupMapUpdateException,
            IllegalStateException {
        return put(key, value, isSharedWrites(), isRemoveOwnedObjectsOnExit(),
                getTimeToLive());
    }

    /**
     * Puts an value into the map associated with the key
     * 
     * @param key
     * @param value
     * @param sharedWrites
     * @param removeOnExit
     * @param timeToLive
     * @return the old value or null
     * @throws GroupMapUpdateException
     * @throws IllegalStateException
     * 
     */
    public V put(K key, V value, boolean sharedWrites, boolean removeOnExit,
            long timeToLive) throws GroupMapUpdateException,
            IllegalStateException {
        checkStatus();
        EntryKey<K> entryKey = new EntryKey<K>(this.local, key);
        EntryValue<V> stateValue = new EntryValue<V>(this.local, value);
        entryKey.setShare(sharedWrites);
        entryKey.setRemoveOnExit(removeOnExit);
        entryKey.setTimeToLive(timeToLive);
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(entryKey);
        entryMsg.setValue(value);
        entryMsg.setType(EntryMessage.MessageType.INSERT);
        return (V) sendRequest(getCoordinator(), entryMsg);
    }

    /**
     * Add the Map to the distribution
     * 
     * @param t
     * @throws GroupMapUpdateException
     * @throws IllegalStateException
     */
    public void putAll(Map<? extends K, ? extends V> t)
            throws GroupMapUpdateException, IllegalStateException {
        putAll(t, isSharedWrites(), isRemoveOwnedObjectsOnExit(),
                getTimeToLive());
    }

    /**
     * Add the Map to the distribution
     * 
     * @param t
     * @param sharedWrites
     * @param removeOnExit
     * @param timeToLive
     * @throws GroupMapUpdateException
     * @throws IllegalStateException
     */
    public void putAll(Map<? extends K, ? extends V> t, boolean sharedWrites,
            boolean removeOnExit, long timeToLive)
            throws GroupMapUpdateException, IllegalStateException {
        for (java.util.Map.Entry<? extends K, ? extends V> entry : t.entrySet()) {
            put(entry.getKey(), entry.getValue(), sharedWrites, removeOnExit,
                    timeToLive);
        }
    }

    /**
     * remove a value from the map associated with the key
     * 
     * @param key
     * @return the Value or null
     * @throws GroupMapUpdateException
     * @throws IllegalStateException
     * 
     */
    public V remove(Object key) throws GroupMapUpdateException,
            IllegalStateException {
        EntryKey<K> entryKey = new EntryKey<K>(this.local, (K) key);
        return remove(entryKey);
    }

    V remove(EntryKey<K> key) throws GroupMapUpdateException,
            IllegalStateException {
        checkStatus();
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(key);
        entryMsg.setType(EntryMessage.MessageType.DELETE);
        return (V) sendRequest(getCoordinator(), entryMsg);
    }

    public int size() {
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap.size() : 0;
        }
    }

    public Collection<V> values() {
        List<V> result = new ArrayList<V>();
        synchronized (this.mapMutex) {
            if (this.localMap != null) {
                for (EntryValue<V> value : this.localMap.values()) {
                    result.add(value.getValue());
                }
            }
        }
        return result;
    }

    /**
     * @return a set of the members
     */
    public Set<Member> members() {
        Set<Member> result = new HashSet<Member>();
        result.addAll(this.members.values());
        return result;
    }

    /**
     * @param key
     * @return true if this is the owner of the key
     */
    public boolean isOwner(K key) {
        EntryKey<K> stateKey = new EntryKey<K>(this.local, key);
        EntryValue<V> entryValue = null;
        synchronized (this.mapMutex) {
            entryValue = this.localMap != null ? this.localMap.get(stateKey)
                    : null;
        }
        boolean result = false;
        if (entryValue != null) {
            result = entryValue.getOwner().getId().equals(this.local.getId());
        }
        return result;
    }

    /**
     * Get the owner of a key
     * 
     * @param key
     * @return the owner - or null if the key doesn't exist
     */
    public Member getOwner(K key) {
        EntryKey<K> stateKey = new EntryKey<K>(this.local, key);
        EntryValue<V> entryValue = null;
        synchronized (this.mapMutex) {
            entryValue = this.localMap != null ? this.localMap.get(stateKey)
                    : null;
        }
        return entryValue != null ? entryValue.getOwner() : null;
    }

    /**
     * @return true if the coordinator for the map
     */
    public boolean isCoordinator() {
        return this.local.equals(this.coordinator);
    }

    /**
     * @return the coordinator
     */
    public Member getCoordinator() {
        return this.coordinator;
    }

    /**
     * Select a coordinator - by default, its the member with the lowest
     * lexicographical id
     * 
     * @param members
     * @return
     */
    protected Member selectCordinator(Collection<Member> members) {
        Member result = this.local;
        for (Member member : members) {
            if (result.getId().compareTo(member.getId()) < 0) {
                result = member;
            }
        }
        return result;
    }

    Object sendRequest(Member member, Serializable payload) {
        Object result = null;
        MapRequest request = new MapRequest();
        String id = this.idGenerator.generateId();
        synchronized (this.requests) {
            this.requests.put(id, request);
        }
        try {
            ObjectMessage objMsg = this.session.createObjectMessage(payload);
            objMsg.setJMSReplyTo(this.inboxTopic);
            objMsg.setJMSCorrelationID(id);
            this.producer.send(member.getInBoxDestination(), objMsg);
            result = request.get(getHeartBeatInterval() * 200000);
        } catch (JMSException e) {
            if (this.started.get()) {
                LOG.error("Failed to send request " + payload, e);
            }
        }
        if (result instanceof GroupMapUpdateException) {
            throw (GroupMapUpdateException) result;
        }
        return result;
    }

    void sendAsyncRequest(AsyncMapRequest asyncRequest, Member member,
            Serializable payload) {
        MapRequest request = new MapRequest();
        String id = this.idGenerator.generateId();
        asyncRequest.add(id, request);
        synchronized (this.requests) {
            this.requests.put(id, request);
        }
        try {
            ObjectMessage objMsg = this.session.createObjectMessage(payload);
            objMsg.setJMSReplyTo(this.inboxTopic);
            objMsg.setJMSCorrelationID(id);
            this.producer.send(member.getInBoxDestination(), objMsg);
        } catch (JMSException e) {
            if (this.started.get()) {
                LOG.error("Failed to send async request " + payload, e);
            }
        }
    }

    void sendReply(Object reply, Destination replyTo, String id) {
        try {
            ObjectMessage replyMsg = this.session
                    .createObjectMessage((Serializable) reply);
            replyMsg.setJMSCorrelationID(id);
            this.producer.send(replyTo, replyMsg);
        } catch (JMSException e) {
            LOG.error("Couldn't send reply from co-ordinator", e);
        }
    }

    void broadcastMapUpdate(EntryMessage entry, String correlationId) {
        try {
            EntryMessage copy = entry.copy();
            copy.setMapUpdate(true);
            ObjectMessage objMsg = this.session.createObjectMessage(copy);
            objMsg.setJMSCorrelationID(correlationId);
            this.producer.send(this.topic, objMsg);
        } catch (JMSException e) {
            if (this.started.get()) {
                LOG.error("Failed to send EntryMessage " + entry, e);
            }
        }
    }

    void processMessage(Message message) {
        if (message instanceof ObjectMessage) {
            ObjectMessage objMsg = (ObjectMessage) message;
            try {
                String id = objMsg.getJMSCorrelationID();
                Destination replyTo = objMsg.getJMSReplyTo();
                Object payload = objMsg.getObject();
                if (payload instanceof Member) {
                    handleHeartbeats((Member) payload);
                } else if (payload instanceof EntryMessage) {
                    EntryMessage entryMsg = (EntryMessage) payload;
                    entryMsg = entryMsg.copy();
                    if (entryMsg.isMapUpdate()) {
                        processMapUpdate(entryMsg);
                    } else {
                        processEntryMessage(entryMsg, replyTo, id);
                    }
                } else if (payload instanceof ElectionMessage) {
                    ElectionMessage electionMsg = (ElectionMessage) payload;
                    electionMsg = electionMsg.copy();
                    processElectionMessage(electionMsg, replyTo, id);
                }
                if (id != null) {
                    MapRequest result = null;
                    synchronized (this.requests) {
                        result = this.requests.remove(id);
                    }
                    if (result != null) {
                        result.put(id, objMsg.getObject());
                    }
                }
            } catch (JMSException e) {
                LOG.warn("Failed to process reply: " + message, e);
            }
        }
    }

    void processEntryMessage(EntryMessage entryMsg, Destination replyTo,
            String correlationId) {
        if (isCoordinator()) {
            EntryKey<K> key = entryMsg.getKey();
            EntryValue<V> value = new EntryValue<V>(key.getOwner(),
                    (V) entryMsg.getValue());
            boolean insert = entryMsg.isInsert();
            boolean containsKey = false;
            synchronized (this.mapMutex) {
                containsKey = this.localMap.containsKey(key);
            }
            if (containsKey) {
                Member owner = getOwner((K) key.getKey());
                if (owner.equals(key.getOwner()) || key.isShare()) {
                    EntryValue<V> old = null;
                    if (insert) {
                        synchronized (this.mapMutex) {
                            old = this.localMap.put(key, value);
                        }
                    } else {
                        synchronized (this.mapMutex) {
                            old = this.localMap.remove(key);
                        }
                    }
                    broadcastMapUpdate(entryMsg, correlationId);
                    fireMapChanged(owner, key.getKey(), old.getValue(), value
                            .getValue(), false);
                } else {
                    Serializable reply = new GroupMapUpdateException(
                            "Owned by " + owner);
                    sendReply(reply, replyTo, correlationId);
                }
            } else {
                if (insert) {
                    synchronized (this.mapMutex) {
                        this.localMap.put(key, value);
                    }
                    broadcastMapUpdate(entryMsg, correlationId);
                    fireMapChanged(key.getOwner(), key.getKey(), null, value
                            .getValue(), false);
                } else {
                    sendReply(null, replyTo, correlationId);
                }
            }
        }
    }

    void processMapUpdate(EntryMessage entryMsg) {
        boolean containsKey = false;
        EntryKey<K> key = entryMsg.getKey();
        EntryValue<V> value = new EntryValue<V>(key.getOwner(), (V) entryMsg
                .getValue());
        boolean insert = entryMsg.isInsert()||entryMsg.isSync();
        synchronized (this.mapMutex) {
            containsKey = this.localMap.containsKey(key);
        }
       
        if (!isCoordinator()||entryMsg.isSync()) {
            if (containsKey) {
                Member owner = getOwner((K) key.getKey());
                EntryValue<V> old = null;
                if (insert) {
                    synchronized (this.mapMutex) {
                        old = this.localMap.put(key, value);
                    }
                } else {
                    synchronized (this.mapMutex) {
                        old = this.localMap.remove(key);
                        value.setValue(null);
                    }
                }
                fireMapChanged(owner, key.getKey(), old.getValue(), value
                        .getValue(), entryMsg.isExpired());
            } else {
                if (insert) {
                    synchronized (this.mapMutex) {
                        this.localMap.put(key, value);
                    }
                    fireMapChanged(key.getOwner(), key.getKey(), null, value
                            .getValue(), false);
                }
            }
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
            election(member, true);
            fireMemberStarted(member);
            if (!member.equals(this.local)) {
                sendHeartBeat(member.getInBoxDestination());
            }
            synchronized (this.memberMutex) {
                this.memberMutex.notifyAll();
            }
        }
    }

    void handleConsumerEvents(ConsumerEvent event) {
        if (!event.isStarted()) {
            Member member = this.members.remove(event.getConsumerId()
                    .toString());
            if (member != null) {
                fireMemberStopped(member);
                election(member, false);
            }
        }
    }

    void checkMembership() {
        if (this.started.get() && this.electionFinished.get()) {
            long checkTime = System.currentTimeMillis()
                    - getHeartBeatInterval();
            boolean doElection = false;
            for (Member member : this.members.values()) {
                if (member.getTimeStamp() < checkTime) {
                    LOG.info("Member timestamp expired " + member);
                    this.members.remove(member.getId());
                    fireMemberStopped(member);
                    doElection = true;
                }
            }
            if (doElection) {
                election(null, false);
            }
        }
    }
    
    void expirationSweep() {
        if (isCoordinator() && this.started.get() && this.electionFinished.get()) {
            List<EntryKey> list = null;
            synchronized (this.mapMutex) {
                Map<EntryKey<K>, EntryValue<V>> map = this.localMap;
                if (map != null) {
                    long currentTime = System.currentTimeMillis();
                    for (EntryKey k : map.keySet()) {
                        if (k.isExpired(currentTime)) {
                            if (list == null) {
                                list = new ArrayList<EntryKey>();
                                list.add(k);
                            }
                        }
                    }
                }
            }
            //do the actual removal of entries in a separate thread
            if (list != null) {
                final List<EntryKey> expire = list;
                this.executor.execute(new Runnable() {
                    public void run() {
                        doExpiration(expire);
                    }
                });
            }
        }
        
    }
    
    void doExpiration(List<EntryKey> list) {
        if (this.started.get() && this.electionFinished.get()
                && isCoordinator()) {
            for (EntryKey k : list) {
                EntryValue<V> old = null;
                synchronized (this.mapMutex) {
                    old = this.localMap.remove(k);
                }
                if (old != null) {
                    EntryMessage entryMsg = new EntryMessage();
                    entryMsg.setType(EntryMessage.MessageType.DELETE);
                    entryMsg.setExpired(true);
                    entryMsg.setKey(k);
                    entryMsg.setValue(old.getValue());
                    broadcastMapUpdate(entryMsg, "");
                    fireMapChanged(k.getOwner(), k.getKey(), old.getValue(),
                            null, true);
                }
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
            } catch (javax.jms.IllegalStateException e) {
                // ignore - as we are probably stopping
            } catch (Throwable e) {
                if (this.started.get()) {
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
                entryMsg.setType(EntryMessage.MessageType.SYNC);
                entryMsg.setMapUpdate(true);
                ObjectMessage objMsg = this.session
                        .createObjectMessage(entryMsg);
                if (!member.equals(entry.getKey().getOwner())) {
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
        LOG.info(this.local.getName() + " Member started " + member);
        for (MemberChangedListener l : this.membershipListeners) {
            l.memberStarted(member);
        }
    }

    void fireMemberStopped(Member member) {
        LOG.info(this.local.getName() + " Member stopped " + member);
        for (MemberChangedListener l : this.membershipListeners) {
            l.memberStopped(member);
        }
        // remove all entries owned by the stopped member
        List<EntryKey<K>> tmpList = new ArrayList<EntryKey<K>>();
        boolean mapExists = false;
        synchronized (this.mapMutex) {
            mapExists = this.localMap != null;
            if (mapExists) {
                for (EntryKey<K> entryKey : this.localMap.keySet()) {
                    if (entryKey.getOwner().equals(member)) {
                        if (entryKey.isRemoveOnExit()) {
                            tmpList.add(entryKey);
                        }
                    }
                }
            }
        }
        if (mapExists) {
            for (EntryKey<K> entryKey : tmpList) {
                EntryValue<V> value = null;
                synchronized (this.mapMutex) {
                    value = this.localMap.remove(entryKey);
                }
                fireMapChanged(member, entryKey.getKey(), value.getValue(),
                        null,false);
            }
        }
    }

    void fireMapChanged(final Member owner, final Object key,
            final Object oldValue, final Object newValue, final boolean expired) {
        if (this.started.get() && this.executor != null
                && !this.executor.isShutdown()) {
            this.executor.execute(new Runnable() {
                public void run() {
                    doFireMapChanged(owner, key, oldValue, newValue, expired);
                }
            });
        }
    }

    void doFireMapChanged(Member owner, Object key, Object oldValue,
            Object newValue, boolean expired) {
        for (MapChangedListener l : this.mapChangedListeners) {
            if (oldValue == null) {
                l.mapInsert(owner, key, newValue);
            } else if (newValue == null) {
                l.mapRemove(owner, key, oldValue, expired);
            } else {
                l.mapUpdate(owner, key, oldValue, newValue);
            }
        }
    }

    void election(final Member member, final boolean memberStarted) {
        if (this.started.get() && this.executor != null
                && !this.executor.isShutdown()) {
            this.executor.execute(new Runnable() {
                public void run() {
                    doElection(member, memberStarted);
                }
            });
        }
    }

    void doElection(Member member, boolean memberStarted) {
        if ((member == null || !member.equals(this.local))
                && this.electionFinished.compareAndSet(true, false)) {
            boolean wasCoordinator = isCoordinator() && !isEmpty();
            // call an election
            while (!callElection())
                ;
            List<Member> members = new ArrayList<Member>(this.members.values());
            this.coordinator = selectCordinator(members);
            if (isCoordinator()) {
                broadcastElectionType(ElectionMessage.MessageType.COORDINATOR);
            }
            if (memberStarted && member != null) {
                if (wasCoordinator || isCoordinator() && this.started.get()) {
                    updateNewMemberMap(member);
                }
            }
            if (!this.electionFinished.get()) {
                try {
                    synchronized (this.electionFinished) {
                        this.electionFinished.wait(this.heartBeatInterval * 2);
                    }
                } catch (InterruptedException e) {
                }
            }
            if (!this.electionFinished.get()) {
                // we must be the coordinator
                this.coordinator = this.local;
                this.electionFinished.set(true);
                broadcastElectionType(ElectionMessage.MessageType.COORDINATOR);
            }
        }
    }

    boolean callElection() {
        List<Member> members = new ArrayList<Member>(this.members.values());
        AsyncMapRequest request = new AsyncMapRequest();
        for (Member member : members) {
            if (this.local.getId().compareTo(member.getId()) < 0) {
                ElectionMessage msg = new ElectionMessage();
                msg.setMember(this.local);
                msg.setType(ElectionMessage.MessageType.ELECTION);
                sendAsyncRequest(request, member, msg);
            }
        }
        return request.isSuccess(getHeartBeatInterval());
    }

    void processElectionMessage(ElectionMessage msg, Destination replyTo,
            String correlationId) {
        if (msg.isElection()) {
            msg.setType(ElectionMessage.MessageType.ANSWER);
            msg.setMember(this.local);
            sendReply(msg, replyTo, correlationId);
        } else if (msg.isCoordinator()) {
            synchronized (this.electionFinished) {
                this.coordinator = msg.getMember();
                this.electionFinished.set(true);
                this.electionFinished.notifyAll();
            }
        }
    }

    void broadcastElectionType(ElectionMessage.MessageType type) {
        if (started.get()) {
            try {
                ElectionMessage msg = new ElectionMessage();
                msg.setMember(this.local);
                msg.setType(type);
                ObjectMessage objMsg = this.session.createObjectMessage(msg);
                this.producer.send(this.topic, objMsg);
            } catch (javax.jms.IllegalStateException e) {
                // ignore - we are stopping
            } catch (JMSException e) {
                if (this.started.get()) {
                    LOG.error("Failed to broadcast election message: " + type,
                            e);
                }
            }
        }
    }

    void checkStatus() throws IllegalStateException {
        if (!started.get()) {
            throw new IllegalStateException("GroupMap " + this.local.getName()
                    + " not started");
        }
        waitForElection();
    }
    
    void waitForElection() {
        synchronized (this.electionFinished) {
            while (started.get() && !this.electionFinished.get()) {
                try {
                    this.electionFinished.wait(1000);
                } catch (InterruptedException e) {
                    stop();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
