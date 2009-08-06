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
package org.apache.activegroups;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activegroups.command.AsyncMapRequest;
import org.apache.activegroups.command.ElectionMessage;
import org.apache.activegroups.command.EntryKey;
import org.apache.activegroups.command.EntryMessage;
import org.apache.activegroups.command.EntryValue;
import org.apache.activegroups.command.MapRequest;
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
 * A <CODE>Group</CODE> is a distributed collaboration implementation that is used to shared state and process
 * messages amongst a distributed group of other <CODE>Group</CODE> instances. Membership of a group is handled
 * automatically using discovery.
 * <P>
 * The underlying transport is JMS and there are some optimizations that occur for membership if used with ActiveMQ -
 * but <CODE>Group</CODE> can be used with any JMS implementation.
 * 
 * <P>
 * Updates to the group shared map are controlled by a coordinator. The coordinator is elected by the member with the
 * lowest lexicographical id - based on the bully algorithm [Silberschatz et al. 1993]
 * <P>
 * The {@link #selectCordinator(Collection<Member> members)} method may be overridden to implement a custom mechanism
 * for choosing how the coordinator is elected for the map.
 * <P>
 * New <CODE>Group</CODE> instances have their state updated by the coordinator, and coordinator failure is handled
 * automatically within the group.
 * <P>
 * All map updates are totally ordered through the coordinator, whilst read operations happen locally.
 * <P>
 * A <CODE>Group</CODE> supports the concept of owner only updates(write locks), shared updates, entry expiration
 * times and removal on owner exit - all of which are optional. In addition, you can grab and release locks for values
 * in the map, independently of who created them.
 * <P>
 * In addition, members of a group can broadcast messages and implement request/response with other <CODE>Group</CODE>
 * instances.
 * 
 * <P>
 * 
 * @param <K>
 *            the key type
 * @param <V>
 *            the value type
 * 
 */
public class Group<K, V> implements Map<K, V>, Service {
    /**
     * default interval within which to detect a member failure
     */
    public static final long DEFAULT_HEART_BEAT_INTERVAL = 1000;
    private static final long EXPIRATION_SWEEP_INTERVAL = 500;
    private static final Log LOG = LogFactory.getLog(Group.class);
    private static final String STATE_PREFIX = "STATE." + Group.class.getName() + ".";
    private static final String GROUP_MESSAGE_PREFIX = "MESSAGE." + Group.class.getName() + ".";
    private static final String STATE_TYPE = "state";
    private static final String MESSAGE_TYPE = "message";
    private static final String MEMBER_ID_PROPERTY = "memberId";
    protected Member local;
    private final Object mapMutex = new Object();
    private Map<K, EntryValue<V>> localMap;
    Map<String, Member> members = new ConcurrentHashMap<String, Member>();
    private Map<String, MapRequest> stateRequests = new HashMap<String, MapRequest>();
    private Map<String, MapRequest> messageRequests = new HashMap<String, MapRequest>();
    private List<MemberChangedListener> membershipListeners = new CopyOnWriteArrayList<MemberChangedListener>();
    private List<GroupStateChangedListener> mapChangedListeners = new CopyOnWriteArrayList<GroupStateChangedListener>();
    private List<GroupMessageListener> groupMessageListeners = new CopyOnWriteArrayList<GroupMessageListener>();
    private Member coordinator;
    private String groupName;
    private boolean alwaysLock;
    private Connection connection;
    private Session stateSession;
    private Session messageSession;
    private Topic stateTopic;
    private Topic heartBeatTopic;
    private Topic inboxTopic;
    private Topic messageTopic;
    private Queue messageQueue;
    private MessageProducer stateProducer;
    private MessageProducer messageProducer;
    private ConsumerEventSource consumerEvents;
    private AtomicBoolean started = new AtomicBoolean();
    private SchedulerTimerTask heartBeatTask;
    private SchedulerTimerTask checkMembershipTask;
    private SchedulerTimerTask expirationTask;
    private Timer timer;
    private long heartBeatInterval = DEFAULT_HEART_BEAT_INTERVAL;
    private IdGenerator idGenerator = new IdGenerator();
    private boolean removeOwnedObjectsOnExit;
    private boolean releaseLockOnExit = true;
    private int timeToLive;
    private int lockTimeToLive;
    private int minimumGroupSize = 1;
    private int coordinatorWeight = 0;
    private final AtomicBoolean electionFinished = new AtomicBoolean(true);
    private ExecutorService stateExecutor;
    private ExecutorService messageExecutor;
    private ThreadPoolExecutor electionExecutor;
    private final Object memberMutex = new Object();

    /**
     * @param connection
     * @param name
     */
    public Group(Connection connection, String name) {
        this(connection, "default", name);
    }

    /**
     * @param connection
     * @param groupName
     * @param name
     */
    public Group(Connection connection, String groupName, String name) {
        this.connection = connection;
        this.local = new Member(name);
        this.coordinator = this.local;
        this.groupName = groupName;
    }

    /**
     * Set the local map implementation to be used By default its a HashMap - but you could use a Cache for example
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
                    this.localMap = new HashMap<K, EntryValue<V>>();
                }
            }
            this.connection.start();
            this.stateSession = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.messageSession = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.stateProducer = this.stateSession.createProducer(null);
            this.stateProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            this.inboxTopic = this.stateSession.createTemporaryTopic();
            String stateTopicName = STATE_PREFIX + this.groupName;
            this.stateTopic = this.stateSession.createTopic(stateTopicName);
            this.heartBeatTopic = this.stateSession.createTopic(stateTopicName + ".heartbeat");
            String messageDestinationName = GROUP_MESSAGE_PREFIX + this.groupName;
            this.messageTopic = this.messageSession.createTopic(messageDestinationName);
            this.messageQueue = this.messageSession.createQueue(messageDestinationName);
            MessageConsumer privateInbox = this.messageSession.createConsumer(this.inboxTopic);
            MessageConsumer memberChangeConsumer = this.stateSession.createConsumer(this.stateTopic);
            String memberId = null;
            if (memberChangeConsumer instanceof ActiveMQMessageConsumer) {
                memberId = ((ActiveMQMessageConsumer) memberChangeConsumer).getConsumerId().toString();
            } else {
                memberId = this.idGenerator.generateId();
            }
            this.local.setId(memberId);
            this.local.setInBoxDestination(this.inboxTopic);
            this.local.setCoordinatorWeight(getCoordinatorWeight());
            privateInbox.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    processJMSMessage(message);
                }
            });
            memberChangeConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    processJMSMessage(message);
                }
            });
            this.messageProducer = this.messageSession.createProducer(null);
            this.messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            MessageConsumer topicMessageConsumer = this.messageSession.createConsumer(this.messageTopic);
            topicMessageConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    processJMSMessage(message);
                }
            });
            MessageConsumer queueMessageConsumer = this.messageSession.createConsumer(this.messageQueue);
            queueMessageConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    processJMSMessage(message);
                }
            });
            MessageConsumer heartBeatConsumer = this.stateSession.createConsumer(this.heartBeatTopic);
            heartBeatConsumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    handleHeartbeats(message);
                }
            });
            this.consumerEvents = new ConsumerEventSource(this.connection, this.stateTopic);
            this.consumerEvents.setConsumerListener(new ConsumerListener() {
                public void onConsumerEvent(ConsumerEvent event) {
                    handleConsumerEvents(event);
                }
            });
            this.consumerEvents.start();
            this.electionExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
                        public Thread newThread(Runnable runnable) {
                            Thread thread = new Thread(runnable, "Election{" + Group.this.local + "}");
                            thread.setDaemon(true);
                            return thread;
                        }
                    });
            this.stateExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "Group State{" + Group.this.local + "}");
                    thread.setDaemon(true);
                    return thread;
                }
            });
            this.messageExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "Group Messages{" + Group.this.local + "}");
                    thread.setDaemon(true);
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
            this.timer.scheduleAtFixedRate(this.heartBeatTask, getHeartBeatInterval() / 3, getHeartBeatInterval() / 2);
            this.timer.scheduleAtFixedRate(this.checkMembershipTask, getHeartBeatInterval(), getHeartBeatInterval());
            this.timer.scheduleAtFixedRate(this.expirationTask, EXPIRATION_SWEEP_INTERVAL, EXPIRATION_SWEEP_INTERVAL);
            // await for members to join
            long timeout = (long) (this.heartBeatInterval * this.minimumGroupSize * 1.5);
            long deadline = System.currentTimeMillis() + timeout;
            while ((this.members.size() < this.minimumGroupSize || !this.electionFinished.get()) && timeout > 0) {
                synchronized (this.electionFinished) {
                    this.electionFinished.wait(timeout);
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
            if (this.electionExecutor != null) {
                this.electionExecutor.shutdownNow();
            }
            if (this.stateExecutor != null) {
                this.stateExecutor.shutdownNow();
            }
            if (this.messageExecutor != null) {
                this.messageExecutor.shutdownNow();
            }
            try {
                this.consumerEvents.stop();
                this.stateSession.close();
                this.messageSession.close();
                this.connection.close();
            } catch (Exception e) {
                LOG.debug("Caught exception stopping", e);
            }
        }
    }

    /**
     * @return true if started
     */
    public boolean isStarted() {
        return this.started.get();
    }

    /**
     * @return true if there is elections have finished
     */
    public boolean isElectionFinished() {
        return this.electionFinished.get();
    }

    void setElectionFinished(boolean flag) {
        this.electionFinished.set(flag);
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
     * @return true if by default always lock objects (default is false)
     */
    public boolean isAlwaysLock() {
        return this.alwaysLock;
    }

    /**
     * @param alwaysLock -
     *            set true if objects inserted will always be locked (default is false)
     */
    public void setAlwaysLock(boolean alwaysLock) {
        this.alwaysLock = alwaysLock;
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
     * Add a listener for membership changes
     * 
     * @param l
     */
    public void addMemberChangedListener(MemberChangedListener l) {
        this.membershipListeners.add(l);
    }

    /**
     * Remove a listener for membership changes
     * 
     * @param l
     */
    public void removeMemberChangedListener(MemberChangedListener l) {
        this.membershipListeners.remove(l);
    }

    /**
     * Add a listener for map changes
     * 
     * @param l
     */
    public void addMapChangedListener(GroupStateChangedListener l) {
        this.mapChangedListeners.add(l);
    }

    /**
     * Remove a listener for map changes
     * 
     * @param l
     */
    public void removeMapChangedListener(GroupStateChangedListener l) {
        this.mapChangedListeners.remove(l);
    }

    /**
     * Add a listener for group messages
     * 
     * @param l
     */
    public void addGroupMessageListener(GroupMessageListener l) {
        this.groupMessageListeners.add(l);
    }

    /**
     * remove a listener for group messages
     * 
     * @param l
     */
    public void removeGroupMessageListener(GroupMessageListener l) {
        this.groupMessageListeners.remove(l);
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
     * Sets the policy for owned objects in the group If set to true, when this <code>GroupMap<code> stops,
     * any objects it owns will be removed from the group map
     * @param removeOwnedObjectsOnExit the removeOwnedObjectsOnExit to set
     */
    public void setRemoveOwnedObjectsOnExit(boolean removeOwnedObjectsOnExit) {
        this.removeOwnedObjectsOnExit = removeOwnedObjectsOnExit;
    }

    /**
     * @return releaseLockOnExit - true by default
     */
    public boolean isReleaseLockOnExit() {
        return releaseLockOnExit;
    }

    /**
     * set release lock on exit - true by default
     * 
     * @param releaseLockOnExit
     *            the releaseLockOnExit to set
     */
    public void setReleaseLockOnExit(boolean releaseLockOnExit) {
        this.releaseLockOnExit = releaseLockOnExit;
    }

    /**
     * @return the lockTimeToLive
     */
    public int getLockTimeToLive() {
        return lockTimeToLive;
    }

    /**
     * @param lockTimeToLive
     *            the lockTimeToLive to set
     */
    public void setLockTimeToLive(int lockTimeToLive) {
        this.lockTimeToLive = lockTimeToLive;
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
     * @return the coordinatorWeight
     */
    public int getCoordinatorWeight() {
        return this.coordinatorWeight;
    }

    /**
     * @param coordinatorWeight
     *            the coordinatorWeight to set
     */
    public void setCoordinatorWeight(int coordinatorWeight) {
        this.coordinatorWeight = coordinatorWeight;
    }

    /**
     * clear entries from the Map
     * 
     * @throws IllegalStateException
     */
    public void clear() throws IllegalStateException {
        checkStatus();
        if (this.localMap != null && !this.localMap.isEmpty()) {
            Set<K> keys = null;
            synchronized (this.mapMutex) {
                keys = new HashSet<K>(this.localMap.keySet());
            }
            for (K key : keys) {
                remove(key);
            }
        }
        this.localMap.clear();
    }

    public boolean containsKey(Object key) {
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap.containsKey(key) : false;
        }
    }

    public boolean containsValue(Object value) {
        EntryValue entryValue = new EntryValue(null, value);
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap.containsValue(entryValue) : false;
        }
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Map<K, V> result = new HashMap<K, V>();
        synchronized (this.mapMutex) {
            if (this.localMap != null) {
                for (EntryValue<V> entry : this.localMap.values()) {
                    result.put((K) entry.getKey(), entry.getValue());
                }
            }
        }
        return result.entrySet();
    }

    public V get(Object key) {
        EntryValue<V> value = null;
        synchronized (this.mapMutex) {
            value = this.localMap != null ? this.localMap.get(key) : null;
        }
        return value != null ? value.getValue() : null;
    }

    public boolean isEmpty() {
        synchronized (this.mapMutex) {
            return this.localMap != null ? this.localMap.isEmpty() : true;
        }
    }

    public Set<K> keySet() {
        Set<K> result = null;
        synchronized (this.mapMutex) {
            result = new HashSet<K>(this.localMap.keySet());
        }
        return result;
    }

    /**
     * Puts an value into the map associated with the key
     * 
     * @param key
     * @param value
     * @return the old value or null
     * @throws GroupUpdateException
     * @throws IllegalStateException
     * 
     */
    public V put(K key, V value) throws GroupUpdateException, IllegalStateException {
        return put(key, value, isAlwaysLock(), isRemoveOwnedObjectsOnExit(), isReleaseLockOnExit(), getTimeToLive(),
                getLockTimeToLive());
    }

    /**
     * Puts an value into the map associated with the key
     * 
     * @param key
     * @param value
     * @param lock
     * @param removeOnExit
     * @param releaseLockOnExit
     * @param timeToLive
     * @param leaseTime
     * @return the old value or null
     * @throws GroupUpdateException
     * @throws IllegalStateException
     * 
     */
    public V put(K key, V value, boolean lock, boolean removeOnExit, boolean releaseLockOnExit, long timeToLive,
            long leaseTime) throws GroupUpdateException, IllegalStateException {
        checkStatus();
        EntryKey<K> entryKey = new EntryKey<K>(this.local, key);
        entryKey.setLocked(lock);
        entryKey.setRemoveOnExit(removeOnExit);
        entryKey.setReleaseLockOnExit(releaseLockOnExit);
        entryKey.setTimeToLive(timeToLive);
        entryKey.setLockLeaseTime(leaseTime);
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(entryKey);
        entryMsg.setValue(value);
        entryMsg.setType(EntryMessage.MessageType.INSERT);
        return (V) sendStateRequest(getCoordinator(), entryMsg);
    }

    /**
     * Remove a lock on a key
     * 
     * @param key
     * @throws GroupUpdateException
     */
    public void unlock(K key) throws GroupUpdateException {
        EntryKey<K> entryKey = new EntryKey<K>(this.local, key);
        entryKey.setLocked(false);
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(entryKey);
        entryMsg.setLockUpdate(true);
        sendStateRequest(getCoordinator(), entryMsg);
    }

    /**
     * Lock a key in the distributed map
     * 
     * @param key
     * @throws GroupUpdateException
     */
    public void lock(K key) throws GroupUpdateException {
        lock(key, getLockTimeToLive());
    }

    /**
     * Lock a key in the distributed map
     * 
     * @param key
     * @param leaseTime
     * @throws GroupUpdateException
     */
    public void lock(K key, long leaseTime) throws GroupUpdateException {
        EntryKey<K> entryKey = new EntryKey<K>(this.local, key);
        entryKey.setLocked(true);
        entryKey.setLockLeaseTime(leaseTime);
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(entryKey);
        entryMsg.setLockUpdate(true);
        sendStateRequest(getCoordinator(), entryMsg);
    }

    /**
     * Add the Map to the distribution
     * 
     * @param t
     * @throws GroupUpdateException
     * @throws IllegalStateException
     */
    public void putAll(Map<? extends K, ? extends V> t) throws GroupUpdateException, IllegalStateException {
        putAll(t, isAlwaysLock(), isRemoveOwnedObjectsOnExit(), isReleaseLockOnExit(), getTimeToLive(),
                getLockTimeToLive());
    }

    /**
     * Add the Map to the distribution
     * 
     * @param t
     * @param lock
     * @param removeOnExit
     * @param releaseLockOnExit
     * @param timeToLive
     * @param lockTimeToLive
     * @throws GroupUpdateException
     * @throws IllegalStateException
     */
    public void putAll(Map<? extends K, ? extends V> t, boolean lock, boolean removeOnExit, boolean releaseLockOnExit,
            long timeToLive, long lockTimeToLive) throws GroupUpdateException, IllegalStateException {
        for (java.util.Map.Entry<? extends K, ? extends V> entry : t.entrySet()) {
            put(entry.getKey(), entry.getValue(), lock, removeOnExit, releaseLockOnExit, timeToLive, lockTimeToLive);
        }
    }

    /**
     * remove a value from the map associated with the key
     * 
     * @param key
     * @return the Value or null
     * @throws GroupUpdateException
     * @throws IllegalStateException
     * 
     */
    public V remove(Object key) throws GroupUpdateException, IllegalStateException {
        EntryKey<K> entryKey = new EntryKey<K>(this.local, (K) key);
        return doRemove(entryKey);
    }

    V doRemove(EntryKey<K> key) throws GroupUpdateException, IllegalStateException {
        checkStatus();
        EntryMessage entryMsg = new EntryMessage();
        entryMsg.setKey(key);
        entryMsg.setType(EntryMessage.MessageType.DELETE);
        return (V) sendStateRequest(getCoordinator(), entryMsg);
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
    public Set<Member> getMembers() {
        Set<Member> result = new HashSet<Member>();
        result.addAll(this.members.values());
        return result;
    }

    /**
     * Get a member by its unique id
     * 
     * @param id
     * @return
     */
    public Member getMemberById(String id) {
        return this.members.get(id);
    }

    /**
     * Return a member of the Group with the matching name
     * 
     * @param name
     * @return
     */
    public Member getMemberByName(String name) {
        if (name != null) {
            for (Member member : this.members.values()) {
                if (member.getName().equals(name)) {
                    return member;
                }
            }
        }
        return null;
    }

    /**
     * @return the local member that represents this <CODE>Group</CODE> instance
     */
    public Member getLocalMember() {
        return this.local;
    }

    /**
     * @param key
     * @return true if this is the owner of the key
     */
    public boolean isOwner(K key) {
        EntryValue<V> entryValue = null;
        synchronized (this.mapMutex) {
            entryValue = this.localMap != null ? this.localMap.get(key) : null;
        }
        boolean result = false;
        if (entryValue != null) {
            result = entryValue.getKey().getOwner().getId().equals(this.local.getId());
        }
        return result;
    }

    /**
     * Get the owner of a key
     * 
     * @param key
     * @return the owner - or null if the key doesn't exist
     */
    EntryKey getKey(Object key) {
        EntryValue<V> entryValue = null;
        synchronized (this.mapMutex) {
            entryValue = this.localMap != null ? this.localMap.get(key) : null;
        }
        return entryValue != null ? entryValue.getKey() : null;
    }

    /**
     * @return true if the coordinator for the map
     */
    protected boolean isCoordinator() {
        return isCoordinatorMatch() && this.electionFinished.get();
    }

    /**
     * @return true if the coordinator for the map
     */
    protected boolean isCoordinatorMatch() {
        return this.local.equals(this.coordinator);
    }

    /**
     * @return the coordinator
     */
    public Member getCoordinator() {
        return this.coordinator;
    }

    void setCoordinator(Member member) {
        this.coordinator = member;
    }

    /**
     * Broadcast a message to the group
     * 
     * @param message
     * @throws JMSException
     */
    public void broadcastMessage(Object message) throws JMSException {
        checkStatus();
        ObjectMessage objMsg = this.messageSession.createObjectMessage((Serializable) message);
        objMsg.setJMSCorrelationID(this.idGenerator.generateId());
        objMsg.setJMSType(MESSAGE_TYPE);
        objMsg.setStringProperty(MEMBER_ID_PROPERTY, this.local.getId());
        this.messageProducer.send(this.messageTopic, objMsg);
    }

    /**
     * As the group for a response - one will be selected from the group
     * 
     * @param member
     * @param message
     * @param timeout
     *            in milliseconds - a value if 0 means wait until complete
     * @return
     * @throws JMSException
     */
    public Serializable broadcastMessageRequest(Object message, long timeout) throws JMSException {
        checkStatus();
        Object result = null;
        MapRequest request = new MapRequest();
        String id = this.idGenerator.generateId();
        synchronized (this.messageRequests) {
            this.messageRequests.put(id, request);
        }
        ObjectMessage objMsg = this.stateSession.createObjectMessage((Serializable) message);
        objMsg.setJMSReplyTo(this.inboxTopic);
        objMsg.setJMSCorrelationID(id);
        objMsg.setJMSType(MESSAGE_TYPE);
        objMsg.setStringProperty(MEMBER_ID_PROPERTY, this.local.getId());
        this.messageProducer.send(this.messageQueue, objMsg);
        result = request.get(timeout);
        return (Serializable) result;
    }

    /**
     * Send a message to the group - but only the least loaded member will process it
     * 
     * @param message
     * @throws JMSException
     */
    public void sendMessage(Object message) throws JMSException {
        checkStatus();
        ObjectMessage objMsg = this.messageSession.createObjectMessage((Serializable) message);
        objMsg.setJMSCorrelationID(this.idGenerator.generateId());
        objMsg.setJMSType(MESSAGE_TYPE);
        objMsg.setStringProperty(MEMBER_ID_PROPERTY, this.local.getId());
        this.messageProducer.send(this.messageQueue, objMsg);
    }

    /**
     * Send a message to an individual member
     * 
     * @param member
     * @param message
     * @throws JMSException
     */
    public void sendMessage(Member member, Object message) throws JMSException {
        checkStatus();
        ObjectMessage objMsg = this.messageSession.createObjectMessage((Serializable) message);
        objMsg.setJMSCorrelationID(this.idGenerator.generateId());
        objMsg.setJMSType(MESSAGE_TYPE);
        objMsg.setStringProperty(MEMBER_ID_PROPERTY, this.local.getId());
        this.messageProducer.send(member.getInBoxDestination(), objMsg);
    }

    /**
     * Send a request to a member
     * 
     * @param member
     * @param message
     * @param timeout
     *            in milliseconds - a value if 0 means wait until complete
     * @return the request or null
     * @throws JMSException
     */
    public Object sendMessageRequest(Member member, Object message, long timeout) throws JMSException {
        checkStatus();
        Object result = null;
        MapRequest request = new MapRequest();
        String id = this.idGenerator.generateId();
        synchronized (this.messageRequests) {
            this.messageRequests.put(id, request);
        }
        ObjectMessage objMsg = this.stateSession.createObjectMessage((Serializable) message);
        objMsg.setJMSReplyTo(this.inboxTopic);
        objMsg.setJMSCorrelationID(id);
        objMsg.setJMSType(MESSAGE_TYPE);
        objMsg.setStringProperty(MEMBER_ID_PROPERTY, this.local.getId());
        this.messageProducer.send(member.getInBoxDestination(), objMsg);
        result = request.get(timeout);
        return result;
    }

    /**
     * send a response to a message
     * 
     * @param member
     * @param replyId
     * @param message
     * @throws JMSException
     */
    public void sendMessageResponse(Member member, String replyId, Object message) throws JMSException {
        checkStatus();
        ObjectMessage objMsg = this.messageSession.createObjectMessage((Serializable) message);
        objMsg.setJMSCorrelationID(replyId);
        objMsg.setJMSType(MESSAGE_TYPE);
        objMsg.setStringProperty(MEMBER_ID_PROPERTY, this.local.getId());
        this.messageProducer.send(member.getInBoxDestination(), objMsg);
    }

    /**
     * Select a coordinator - coordinator weighting is used - or if everything is equal - a comparison of member ids.
     * 
     * @param members
     * @return
     */
    protected Member selectCordinator(List<Member> list) {
        List<Member> sorted = sortMemberList(list);
        Member result = sorted.isEmpty() ? this.local : sorted.get(list.size() - 1);
        return result;
    }

    protected List<Member> sortMemberList(List<Member> list) {
        Collections.sort(list, new Comparator<Member>() {
            public int compare(Member m1, Member m2) {
                int result = m1.getCoordinatorWeight() - m2.getCoordinatorWeight();
                if (result == 0) {
                    result = m1.getId().compareTo(m2.getId());
                }
                return result;
            }
        });
        return list;
    }

    Object sendStateRequest(Member member, Serializable payload) {
        Object result = null;
        MapRequest request = new MapRequest();
        String id = this.idGenerator.generateId();
        synchronized (this.stateRequests) {
            this.stateRequests.put(id, request);
        }
        try {
            ObjectMessage objMsg = this.stateSession.createObjectMessage(payload);
            objMsg.setJMSReplyTo(this.inboxTopic);
            objMsg.setJMSCorrelationID(id);
            objMsg.setJMSType(STATE_TYPE);
            this.stateProducer.send(member.getInBoxDestination(), objMsg);
            result = request.get(getHeartBeatInterval());
        } catch (JMSException e) {
            if (this.started.get()) {
                LOG.error("Failed to send request " + payload, e);
            }
        }
        if (result instanceof GroupUpdateException) {
            throw (GroupUpdateException) result;
        }
        if (result instanceof EntryMessage) {
            EntryMessage entryMsg = (EntryMessage) result;
            result = entryMsg.getOldValue();
        }
        return result;
    }

    void sendAsyncStateRequest(AsyncMapRequest asyncRequest, Member member, Serializable payload) {
        MapRequest request = new MapRequest();
        String id = this.idGenerator.generateId();
        asyncRequest.add(id, request);
        synchronized (this.stateRequests) {
            this.stateRequests.put(id, request);
        }
        try {
            ObjectMessage objMsg = this.stateSession.createObjectMessage(payload);
            objMsg.setJMSReplyTo(this.inboxTopic);
            objMsg.setJMSCorrelationID(id);
            objMsg.setJMSType(STATE_TYPE);
            this.stateProducer.send(member.getInBoxDestination(), objMsg);
        } catch (JMSException e) {
            if (this.started.get()) {
                LOG.error("Failed to send async request " + payload, e);
            }
        }
    }

    void sendReply(Object reply, Destination replyTo, String id) {
        if (this.started.get()) {
            if (replyTo != null) {
                if (replyTo.equals(this.local.getInBoxDestination())) {
                    processRequest(id, reply);
                } else {
                    try {
                        ObjectMessage replyMsg = this.stateSession.createObjectMessage((Serializable) reply);
                        replyMsg.setJMSCorrelationID(id);
                        replyMsg.setJMSType(STATE_TYPE);
                        this.stateProducer.send(replyTo, replyMsg);
                    } catch (JMSException e) {
                        LOG.error("Couldn't send reply from co-ordinator", e);
                    }
                }
            } else {
                LOG.error("NULL replyTo destination");
            }
        }
    }

    void broadcastMapUpdate(EntryMessage entry, String correlationId) {
        if (this.started.get()) {
            try {
                EntryMessage copy = entry.copy();
                copy.setMapUpdate(true);
                ObjectMessage objMsg = this.stateSession.createObjectMessage(copy);
                objMsg.setJMSCorrelationID(correlationId);
                objMsg.setJMSType(STATE_TYPE);
                this.stateProducer.send(this.stateTopic, objMsg);
            } catch (JMSException e) {
                if (this.started.get()) {
                    LOG.error("Failed to send EntryMessage " + entry, e);
                }
            }
        }
    }

    void processJMSMessage(Message message) {
        if (message instanceof ObjectMessage) {
            ObjectMessage objMsg = (ObjectMessage) message;
            try {
                String messageType = objMsg.getJMSType();
                String id = objMsg.getJMSCorrelationID();
                String memberId = objMsg.getStringProperty(MEMBER_ID_PROPERTY);
                Destination replyTo = objMsg.getJMSReplyTo();
                Object payload = objMsg.getObject();
                if (messageType != null) {
                    if (messageType.equals(STATE_TYPE)) {
                        if (payload instanceof Member) {
                            handleHeartbeats((Member) payload);
                        } else if (payload instanceof EntryMessage) {
                            EntryMessage entryMsg = (EntryMessage) payload;
                            entryMsg = entryMsg.copy();
                            if (entryMsg.isLockUpdate()) {
                                processLockUpdate(entryMsg, replyTo, id);
                            } else if (entryMsg.isMapUpdate()) {
                                processMapUpdate(entryMsg);
                            } else {
                                processEntryMessage(entryMsg, replyTo, id);
                            }
                        } else if (payload instanceof ElectionMessage) {
                            ElectionMessage electionMsg = (ElectionMessage) payload;
                            electionMsg = electionMsg.copy();
                            processElectionMessage(electionMsg, replyTo, id);
                        }
                    } else if (messageType.equals(MESSAGE_TYPE)) {
                        processGroupMessage(memberId, id, replyTo, payload);
                    } else {
                        LOG.error("Unknown message type: " + messageType);
                    }
                    processRequest(id, payload);
                } else {
                    LOG.error("Can't process a message type of null");
                }
            } catch (JMSException e) {
                LOG.warn("Failed to process message: " + message, e);
            }
        }
    }

    void processRequest(String id, Object value) {
        if (id != null) {
            MapRequest result = null;
            synchronized (this.stateRequests) {
                result = this.stateRequests.remove(id);
            }
            if (result != null) {
                result.put(id, value);
            }
        }
    }

    void processLockUpdate(EntryMessage entryMsg, Destination replyTo, String correlationId) {
        waitForElection();
        synchronized (this.mapMutex) {
            boolean newLock = entryMsg.getKey().isLocked();
            Member newOwner = entryMsg.getKey().getOwner();
            long newLockExpiration = newLock ? entryMsg.getKey().getLockExpiration() : 0l;
            if (isCoordinator() && !entryMsg.isMapUpdate()) {
                EntryKey originalKey = getKey(entryMsg.getKey().getKey());
                if (originalKey != null) {
                    if (originalKey.isLocked()) {
                        if (!originalKey.getOwner().equals(entryMsg.getKey().getOwner())) {
                            Serializable reply = new GroupUpdateException("Owned by " + originalKey.getOwner());
                            sendReply(reply, replyTo, correlationId);
                        } else {
                            originalKey.setLocked(newLock);
                            originalKey.setOwner(newOwner);
                            originalKey.setLockExpiration(newLockExpiration);
                            broadcastMapUpdate(entryMsg, correlationId);
                        }
                    } else {
                        originalKey.setLocked(newLock);
                        originalKey.setOwner(newOwner);
                        originalKey.setLockExpiration(newLockExpiration);
                        broadcastMapUpdate(entryMsg, correlationId);
                    }
                }
            } else {
                EntryKey originalKey = getKey(entryMsg.getKey().getKey());
                if (originalKey != null) {
                    originalKey.setLocked(newLock);
                    originalKey.setOwner(newOwner);
                    originalKey.setLockExpiration(newLockExpiration);
                }
            }
        }
    }

    void processEntryMessage(EntryMessage entryMsg, Destination replyTo, String correlationId) {
        waitForElection();
        if (isCoordinator()) {
            EntryKey<K> key = entryMsg.getKey();
            EntryValue<V> value = new EntryValue<V>(key, (V) entryMsg.getValue());
            boolean insert = entryMsg.isInsert();
            boolean containsKey = false;
            synchronized (this.mapMutex) {
                containsKey = this.localMap.containsKey(key.getKey());
            }
            if (containsKey) {
                EntryKey originalKey = getKey(key.getKey());
                if (originalKey.equals(key.getOwner()) || !originalKey.isLocked()) {
                    EntryValue<V> old = null;
                    if (insert) {
                        synchronized (this.mapMutex) {
                            old = this.localMap.put(key.getKey(), value);
                        }
                    } else {
                        synchronized (this.mapMutex) {
                            old = this.localMap.remove(key.getKey());
                        }
                    }
                    entryMsg.setOldValue(old.getValue());
                    broadcastMapUpdate(entryMsg, correlationId);
                    fireMapChanged(key.getOwner(), key.getKey(), old.getValue(), value.getValue(), false);
                } else {
                    Serializable reply = new GroupUpdateException("Owned by " + originalKey.getOwner());
                    sendReply(reply, replyTo, correlationId);
                }
            } else {
                if (insert) {
                    synchronized (this.mapMutex) {
                        this.localMap.put(key.getKey(), value);
                    }
                    broadcastMapUpdate(entryMsg, correlationId);
                    fireMapChanged(key.getOwner(), key.getKey(), null, value.getValue(), false);
                } else {
                    sendReply(null, replyTo, correlationId);
                }
            }
        }
    }

    void processMapUpdate(EntryMessage entryMsg) {
        boolean containsKey = false;
        EntryKey<K> key = entryMsg.getKey();
        EntryValue<V> value = new EntryValue<V>(key, (V) entryMsg.getValue());
        boolean insert = entryMsg.isInsert() || entryMsg.isSync();
        synchronized (this.mapMutex) {
            containsKey = this.localMap.containsKey(key.getKey());
        }
        waitForElection();
        if (!isCoordinator() || entryMsg.isSync()) {
            if (containsKey) {
                if (key.isLockExpired()) {
                    EntryValue old = this.localMap.get(key.getKey());
                    if (old != null) {
                        old.getKey().setLocked(false);
                    }
                } else {
                    EntryValue<V> old = null;
                    if (insert) {
                        synchronized (this.mapMutex) {
                            old = this.localMap.put(key.getKey(), value);
                        }
                    } else {
                        synchronized (this.mapMutex) {
                            old = this.localMap.remove(key.getKey());
                            value.setValue(null);
                        }
                    }
                    fireMapChanged(key.getOwner(), key.getKey(), old.getValue(), value.getValue(), entryMsg.isExpired());
                }
            } else {
                if (insert) {
                    synchronized (this.mapMutex) {
                        this.localMap.put(key.getKey(), value);
                    }
                    fireMapChanged(key.getOwner(), key.getKey(), null, value.getValue(), false);
                }
            }
        }
    }

    void processGroupMessage(String memberId, String replyId, Destination replyTo, Object payload) {
        Member member = this.members.get(memberId);
        if (member != null) {
            fireMemberMessage(member, replyId, payload);
        }
        if (replyId != null) {
            MapRequest result = null;
            synchronized (this.messageRequests) {
                result = this.messageRequests.remove(replyId);
            }
            if (result != null) {
                result.put(replyId, payload);
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
            fireMemberStarted(member);
            if (!member.equals(this.local)) {
                sendHeartBeat(member.getInBoxDestination());
            }
            election(member, true);
            synchronized (this.memberMutex) {
                this.memberMutex.notifyAll();
            }
        }
    }

    void handleConsumerEvents(ConsumerEvent event) {
        if (!event.isStarted()) {
            Member member = this.members.remove(event.getConsumerId().toString());
            if (member != null) {
                fireMemberStopped(member);
                election(member, false);
            }
        }
    }

    void checkMembership() {
        if (this.started.get() && this.electionFinished.get()) {
            long checkTime = System.currentTimeMillis() - getHeartBeatInterval();
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
        waitForElection();
        if (isCoordinator() && this.started.get() && this.electionFinished.get()) {
            List<EntryKey> expiredMessages = null;
            List<EntryKey> expiredLocks = null;
            synchronized (this.mapMutex) {
                Map<K, EntryValue<V>> map = this.localMap;
                if (map != null) {
                    long currentTime = System.currentTimeMillis();
                    for (EntryValue value : map.values()) {
                        EntryKey k = value.getKey();
                        if (k.isExpired(currentTime)) {
                            if (expiredMessages == null) {
                                expiredMessages = new ArrayList<EntryKey>();
                            }
                            expiredMessages.add(k);
                        } else if (k.isLockExpired(currentTime)) {
                            k.setLocked(false);
                            if (expiredLocks == null) {
                                expiredLocks = new ArrayList<EntryKey>();
                            }
                            expiredLocks.add(k);
                        }
                    }
                }
            }
            // do the actual removal of entries in a separate thread
            if (expiredMessages != null) {
                final List<EntryKey> expire = expiredMessages;
                this.stateExecutor.execute(new Runnable() {
                    public void run() {
                        doMessageExpiration(expire);
                    }
                });
            }
            if (expiredLocks != null) {
                final List<EntryKey> expire = expiredLocks;
                this.stateExecutor.execute(new Runnable() {
                    public void run() {
                        doLockExpiration(expire);
                    }
                });
            }
        }
    }

    void doMessageExpiration(List<EntryKey> list) {
        if (this.started.get() && this.electionFinished.get() && isCoordinator()) {
            for (EntryKey k : list) {
                EntryValue<V> old = null;
                synchronized (this.mapMutex) {
                    old = this.localMap.remove(k.getKey());
                }
                if (old != null) {
                    EntryMessage entryMsg = new EntryMessage();
                    entryMsg.setType(EntryMessage.MessageType.DELETE);
                    entryMsg.setExpired(true);
                    entryMsg.setKey(k);
                    entryMsg.setValue(old.getValue());
                    broadcastMapUpdate(entryMsg, "");
                    fireMapChanged(k.getOwner(), k.getKey(), old.getValue(), null, true);
                }
            }
        }
    }

    void doLockExpiration(List<EntryKey> list) {
        if (this.started.get() && this.electionFinished.get() && isCoordinator()) {
            for (EntryKey k : list) {
                EntryMessage entryMsg = new EntryMessage();
                entryMsg.setType(EntryMessage.MessageType.DELETE);
                entryMsg.setLockExpired(true);
                entryMsg.setKey(k);
                broadcastMapUpdate(entryMsg, "");
            }
        }
    }

    void sendHeartBeat() {
        sendHeartBeat(this.heartBeatTopic);
    }

    void sendHeartBeat(Destination destination) {
        if (this.started.get()) {
            try {
                ObjectMessage msg = this.stateSession.createObjectMessage(this.local);
                msg.setJMSType(STATE_TYPE);
                this.stateProducer.send(destination, msg);
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
        List<Map.Entry<K, EntryValue<V>>> list = new ArrayList<Map.Entry<K, EntryValue<V>>>();
        synchronized (this.mapMutex) {
            if (this.localMap != null) {
                for (Map.Entry<K, EntryValue<V>> entry : this.localMap.entrySet()) {
                    list.add(entry);
                }
            }
        }
        try {
            for (Map.Entry<K, EntryValue<V>> entry : list) {
                EntryMessage entryMsg = new EntryMessage();
                entryMsg.setKey(entry.getValue().getKey());
                entryMsg.setValue(entry.getValue().getValue());
                entryMsg.setType(EntryMessage.MessageType.SYNC);
                entryMsg.setMapUpdate(true);
                ObjectMessage objMsg = this.stateSession.createObjectMessage(entryMsg);
                if (!member.equals(entry.getValue().getKey().getOwner())) {
                    objMsg.setJMSType(STATE_TYPE);
                    this.stateProducer.send(member.getInBoxDestination(), objMsg);
                }
            }
        } catch (javax.jms.IllegalStateException e) {
            // ignore - as closing
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
                for (EntryValue value : this.localMap.values()) {
                    EntryKey entryKey = value.getKey();
                    if (entryKey.getOwner().equals(member)) {
                        if (entryKey.isRemoveOnExit()) {
                            tmpList.add(entryKey);
                        }
                        if (entryKey.isReleaseLockOnExit()) {
                            entryKey.setLocked(false);
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
                fireMapChanged(member, entryKey.getKey(), value.getValue(), null, false);
            }
        }
    }

    void fireMemberMessage(final Member member, final String replyId, final Object message) {
        if (this.started.get() && this.stateExecutor != null && !this.messageExecutor.isShutdown()) {
            this.messageExecutor.execute(new Runnable() {
                public void run() {
                    doFireMemberMessage(member, replyId, message);
                }
            });
        }
    }

    void doFireMemberMessage(Member sender, String replyId, Object message) {
        if (this.started.get()) {
            for (GroupMessageListener l : this.groupMessageListeners) {
                l.messageDelivered(sender, replyId, message);
            }
        }
    }

    void fireMapChanged(final Member owner, final Object key, final Object oldValue, final Object newValue,
            final boolean expired) {
        if (this.started.get() && this.stateExecutor != null && !this.stateExecutor.isShutdown()) {
            this.stateExecutor.execute(new Runnable() {
                public void run() {
                    doFireMapChanged(owner, key, oldValue, newValue, expired);
                }
            });
        }
    }

    void doFireMapChanged(Member owner, Object key, Object oldValue, Object newValue, boolean expired) {
        if (this.started.get()) {
            for (GroupStateChangedListener l : this.mapChangedListeners) {
                if (oldValue == null) {
                    l.mapInsert(owner, key, newValue);
                } else if (newValue == null) {
                    l.mapRemove(owner, key, oldValue, expired);
                } else {
                    l.mapUpdate(owner, key, oldValue, newValue);
                }
            }
        }
    }

    void checkStatus() throws IllegalStateException {
        if (!started.get()) {
            throw new IllegalStateException("GroupMap " + this.local.getName() + " not started");
        }
        waitForElection();
    }

    public String toString() {
        return "Group:" + getName() + "{id=" + this.local.getId() + ",coordinator=" + isCoordinator() + ",inbox="
                + this.local.getInBoxDestination() + "}";
    }

    void election(final Member member, final boolean memberStarted) {
        if (this.started.get() && this.electionExecutor != null && !this.electionExecutor.isShutdown()) {
            synchronized (this.electionFinished) {
                this.electionFinished.set(false);
            }
            synchronized (this.electionExecutor) {
                // remove any queued election tasks
                List<Runnable> list = new ArrayList<Runnable>(this.electionExecutor.getQueue());
                for (Runnable r : list) {
                    ElectionService es = (ElectionService) r;
                    es.stop();
                    this.electionExecutor.remove(es);
                }
            }
            ElectionService es = new ElectionService(member, memberStarted);
            es.start();
            this.electionExecutor.execute(es);
        }
    }

    boolean callElection() {
        List<Member> members = new ArrayList<Member>(this.members.values());
        List<Member> sorted = sortMemberList(members);
        AsyncMapRequest request = new AsyncMapRequest();
        boolean doCall = false;
        for (Member member : sorted) {
            if (this.local.equals(member)) {
                doCall = true;
            } else if (doCall) {
                ElectionMessage msg = new ElectionMessage();
                msg.setMember(this.local);
                msg.setType(ElectionMessage.MessageType.ELECTION);
                sendAsyncStateRequest(request, member, msg);
            }
        }
        boolean result = request.isSuccess(getHeartBeatInterval());
        return result;
    }

    void processElectionMessage(ElectionMessage msg, Destination replyTo, String correlationId) {
        if (msg.isElection()) {
            msg.setType(ElectionMessage.MessageType.ANSWER);
            msg.setMember(this.local);
            sendReply(msg, replyTo, correlationId);
            election(null, false);
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
                ObjectMessage objMsg = this.stateSession.createObjectMessage(msg);
                objMsg.setJMSType(STATE_TYPE);
                this.stateProducer.send(this.stateTopic, objMsg);
            } catch (javax.jms.IllegalStateException e) {
                // ignore - we are stopping
            } catch (JMSException e) {
                if (this.started.get()) {
                    LOG.error("Failed to broadcast election message: " + type, e);
                }
            }
        }
    }

    void waitForElection() {
        synchronized (this.electionFinished) {
            while (started.get() && !this.electionFinished.get()) {
                try {
                    this.electionFinished.wait(200);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted in waitForElection");
                    stop();
                }
            }
        }
    }
    class ElectionService implements Runnable {
        private AtomicBoolean started = new AtomicBoolean();
        private Member member;
        private boolean memberStarted;

        ElectionService(Member member, boolean memberStarted) {
            this.member = member;
            this.memberStarted = memberStarted;
        }

        void start() {
            this.started.set(true);
        }

        void stop() {
            this.started.set(false);
        }

        public void run() {
            doElection();
        }

        void doElection() {
            if ((this.member == null || (!this.member.equals(Group.this.local) || Group.this.members.size() == getMinimumGroupSize()))) {
                boolean wasCoordinator = isCoordinatorMatch() && !isEmpty();
                // call an election
                while (!callElection() && isStarted() && this.started.get())
                    ;
                if (isStarted() && this.started.get()) {
                    List<Member> members = new ArrayList<Member>(Group.this.members.values());
                    Group.this.coordinator = selectCordinator(members);
                    if (isCoordinatorMatch()) {
                        broadcastElectionType(ElectionMessage.MessageType.COORDINATOR);
                    }
                    if (this.memberStarted && this.member != null) {
                        if (wasCoordinator || isCoordinator() && this.started.get()) {
                            updateNewMemberMap(this.member);
                        }
                    }
                    if (!isElectionFinished() && this.started.get()) {
                        try {
                            synchronized (Group.this.electionFinished) {
                                Group.this.electionFinished.wait(Group.this.heartBeatInterval * 2);
                            }
                        } catch (InterruptedException e) {
                        }
                    }
                    if (!isElectionFinished() && this.started.get()) {
                        // we must be the coordinator
                        setCoordinator(getLocalMember());
                        setElectionFinished(true);
                        broadcastElectionType(ElectionMessage.MessageType.COORDINATOR);
                    }
                }
            }
        }
    }
}
