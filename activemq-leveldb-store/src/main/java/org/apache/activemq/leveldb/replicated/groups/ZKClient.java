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
package org.apache.activemq.leveldb.replicated.groups;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.linkedin.util.clock.Clock;
import org.linkedin.util.clock.SystemClock;
import org.linkedin.util.clock.Timespan;
import org.linkedin.util.concurrent.ConcurrentUtils;
import org.linkedin.util.io.PathUtils;
import org.linkedin.zookeeper.client.ChrootedZKClient;
import org.linkedin.zookeeper.client.IZooKeeper;
import org.linkedin.zookeeper.client.IZooKeeperFactory;
import org.linkedin.zookeeper.client.LifecycleListener;
import org.linkedin.zookeeper.client.ZooKeeperFactory;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.ConfigurationException;
import org.slf4j.Logger;

public class ZKClient extends org.linkedin.zookeeper.client.AbstractZKClient implements Watcher {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ZKClient.class.getName());

    private Map<String, String> acls;
    private String password;


    public void start() throws Exception {
        // Grab the lock to make sure that the registration of the ManagedService
        // won't be updated immediately but that the initial update will happen first
        synchronized (_lock) {
            _stateChangeDispatcher.setDaemon(true);
            _stateChangeDispatcher.start();

            doStart();
        }
    }

    public void setACLs(Map<String, String> acls) {
        this.acls = acls;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    protected void doStart() throws InvalidSyntaxException, ConfigurationException, UnsupportedEncodingException {
        connect();
    }

    @Override
    public void close() {
        if (_stateChangeDispatcher != null) {
            _stateChangeDispatcher.end();
            try {
                _stateChangeDispatcher.join(1000);
            } catch(Exception e) {
                LOG.debug("ignored exception", e);
            }
        }
        synchronized(_lock) {
            if (_zk != null) {
                try {
                    changeState(State.NONE);
                    _zk.close();
                    // We try to avoid a NPE when shutting down fabric:
                    // java.lang.NullPointerException
                    //     at org.apache.felix.framework.BundleWiringImpl.findClassOrResourceByDelegation(BundleWiringImpl.java:1433)
                    //     at org.apache.felix.framework.BundleWiringImpl.access$400(BundleWiringImpl.java:73)
                    //     at org.apache.felix.framework.BundleWiringImpl$BundleClassLoader.loadClass(BundleWiringImpl.java:1844)
                    //     at java.lang.ClassLoader.loadClass(ClassLoader.java:247)
                    //     at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1089)
                    Thread th = getSendThread();
                    if (th != null) {
                        th.join(1000);
                    }
                    _zk = null;
                } catch(Exception e) {
                    LOG.debug("ignored exception", e);
                }
            }
        }
    }

    protected Thread getSendThread() {
        try {
            return (Thread) getField(_zk, "_zk", "cnxn", "sendThread");
        } catch (Throwable e) {
            return null;
        }
    }

    protected Object getField(Object obj, String... names) throws Exception {
        for (String name : names) {
            obj = getField(obj, name);
        }
        return obj;
    }

    protected Object getField(Object obj, String name) throws Exception {
        Class clazz = obj.getClass();
        while (clazz != null) {
            for (Field f : clazz.getDeclaredFields()) {
                if (f.getName().equals(name)) {
                    f.setAccessible(true);
                    return f.get(obj);
                }
            }
        }
        throw new NoSuchFieldError(name);
    }

    protected void changeState(State newState) {
        synchronized (_lock) {
            State oldState = _state;
            if (oldState != newState) {
                _stateChangeDispatcher.addEvent(oldState, newState);
                _state = newState;
                _lock.notifyAll();
            }
        }
    }

    public void testGenerateConnectionLoss() throws Exception {
        waitForConnected();
        Object clientCnxnSocket  = getField(_zk, "_zk", "cnxn", "sendThread", "clientCnxnSocket");
        callMethod(clientCnxnSocket, "testableCloseSocket");
    }

    protected Object callMethod(Object obj, String name, Object... args) throws Exception {
        Class clazz = obj.getClass();
        while (clazz != null) {
            for (Method m : clazz.getDeclaredMethods()) {
                if (m.getName().equals(name)) {
                    m.setAccessible(true);
                    return m.invoke(obj, args);
                }
            }
        }
        throw new NoSuchMethodError(name);
    }

    protected void tryConnect() {
        synchronized (_lock) {
            try {
                connect();
            } catch (Throwable e) {
                LOG.warn("Error while restarting:", e);
                if (_expiredSessionRecovery == null) {
                    _expiredSessionRecovery = new ExpiredSessionRecovery();
                    _expiredSessionRecovery.setDaemon(true);
                    _expiredSessionRecovery.start();
                }
            }
        }
    }

    public void connect() throws UnsupportedEncodingException {
        synchronized (_lock) {
            changeState(State.CONNECTING);
            _zk = _factory.createZooKeeper(this);
            if (password != null) {
                _zk.addAuthInfo("digest", ("fabric:" + password).getBytes("UTF-8"));
            }
        }
    }

    public void process(WatchedEvent event) {
        if (event.getState() != null) {
            LOG.debug("event: {}", event.getState());
            synchronized (_lock) {
                switch(event.getState())
                {
                    case SyncConnected:
                        changeState(State.CONNECTED);
                        break;

                    case Disconnected:
                        if(_state != State.NONE) {
                            changeState(State.RECONNECTING);
                        }
                        break;

                    case Expired:
                        // when expired, the zookeeper object is invalid and we need to recreate a new one
                        _zk = null;
                        LOG.warn("Expiration detected: trying to restart...");
                        tryConnect();
                        break;
                    default:
                        LOG.warn("unprocessed event state: {}", event.getState());
                }
            }
        }
    }

    @Override
    protected IZooKeeper getZk() {
        State state = _state;
        if (state == State.NONE) {
            throw new IllegalStateException("ZooKeeper client has not been configured yet. You need to either create an ensemble or join one.");
        } else if (state != State.CONNECTED) {
            try {
                waitForConnected();
            } catch (Exception e) {
                throw new IllegalStateException("Error waiting for ZooKeeper connection", e);
            }
        }
        IZooKeeper zk = _zk;
        if (zk == null) {
            throw new IllegalStateException("No ZooKeeper connection available");
        }
        return zk;
    }

    public void waitForConnected(Timespan timeout) throws InterruptedException, TimeoutException {
        waitForState(State.CONNECTED, timeout);
    }

    public void waitForConnected() throws InterruptedException, TimeoutException {
        waitForConnected(null);
    }

    public void waitForState(State state, Timespan timeout) throws TimeoutException, InterruptedException {
        long endTime = (timeout == null ? sessionTimeout : timeout).futureTimeMillis(_clock);
        if (_state != state) {
            synchronized (_lock) {
                while (_state != state) {
                    ConcurrentUtils.awaitUntil(_clock, _lock, endTime);
                }
            }
        }
    }

    @Override
    public void registerListener(LifecycleListener listener) {
        if (listener == null) {
            throw new IllegalStateException("listener is null");
        }
        if (!_listeners.contains(listener)) {
            _listeners.add(listener);

        }
        if (_state == State.CONNECTED) {
            listener.onConnected();
            //_stateChangeDispatcher.addEvent(null, State.CONNECTED);
        }
    }

    @Override
    public void removeListener(LifecycleListener listener) {
        if (listener == null) {
            throw new IllegalStateException("listener is null");
        }
        _listeners.remove(listener);
    }

    @Override
    public org.linkedin.zookeeper.client.IZKClient chroot(String path) {
        return new ChrootedZKClient(this, adjustPath(path));
    }

    @Override
    public boolean isConnected() {
        return _state == State.CONNECTED;
    }

    public boolean isConfigured() {
        return _state != State.NONE;
    }

    @Override
    public String getConnectString() {
        return _factory.getConnectString();
    }

    public static enum State {
        NONE,
        CONNECTING,
        CONNECTED,
        RECONNECTING
    }

    private final static String CHARSET = "UTF-8";

    private final Clock _clock = SystemClock.instance();
    private final List<LifecycleListener> _listeners = new CopyOnWriteArrayList<LifecycleListener>();

    protected final Object _lock = new Object();
    protected volatile State _state = State.NONE;

    private final StateChangeDispatcher _stateChangeDispatcher = new StateChangeDispatcher();

    protected IZooKeeperFactory _factory;
    protected IZooKeeper _zk;
    protected Timespan _reconnectTimeout = Timespan.parse("20s");
    protected Timespan sessionTimeout = new Timespan(30, Timespan.TimeUnit.SECOND);

    private ExpiredSessionRecovery _expiredSessionRecovery = null;

    private class StateChangeDispatcher extends Thread {
        private final AtomicBoolean _running = new AtomicBoolean(true);
        private final BlockingQueue<Boolean> _events = new LinkedBlockingQueue<Boolean>();

        private StateChangeDispatcher() {
            super("ZooKeeper state change dispatcher thread");
        }

        @Override
        public void run() {
            Map<Object, Boolean> history = new IdentityHashMap<Object, Boolean>();
            LOG.info("Starting StateChangeDispatcher");
            while (_running.get()) {
                Boolean isConnectedEvent;
                try {
                    isConnectedEvent = _events.take();
                } catch (InterruptedException e) {
                    continue;
                }
                if (!_running.get() || isConnectedEvent == null) {
                    continue;
                }
                Map<Object, Boolean> newHistory = callListeners(history, isConnectedEvent);
                // we save which event each listener has seen last
                // we don't update the map in place because we need to get rid of unregistered listeners
                history = newHistory;
            }
            LOG.info("StateChangeDispatcher terminated.");
        }

        public void end() {
            _running.set(false);
            _events.add(false);
        }

        public void addEvent(ZKClient.State oldState, ZKClient.State newState) {
            LOG.debug("addEvent: {} => {}", oldState, newState);
            if (newState == ZKClient.State.CONNECTED) {
                _events.add(true);
            } else if (oldState == ZKClient.State.CONNECTED) {
                _events.add(false);
            }
        }
    }

    protected Map<Object, Boolean> callListeners(Map<Object, Boolean> history, Boolean connectedEvent) {
        Map<Object, Boolean> newHistory = new IdentityHashMap<Object, Boolean>();
        for (LifecycleListener listener : _listeners) {
            Boolean previousEvent = history.get(listener);
            // we propagate the event only if it was not already sent
            if (previousEvent == null || previousEvent != connectedEvent) {
                try {
                    if (connectedEvent) {
                        listener.onConnected();
                    } else {
                        listener.onDisconnected();
                    }
                } catch (Throwable e) {
                    LOG.warn("Exception while executing listener (ignored)", e);
                }
            }
            newHistory.put(listener, connectedEvent);
        }
        return newHistory;
    }

    private class ExpiredSessionRecovery extends Thread {
        private ExpiredSessionRecovery() {
            super("ZooKeeper expired session recovery thread");
        }

        @Override
        public void run() {
            LOG.info("Entering recovery mode");
            synchronized(_lock) {
                try {
                    int count = 0;
                    while (_state == ZKClient.State.NONE) {
                        try {
                            count++;
                            LOG.warn("Recovery mode: trying to reconnect to zookeeper [" + count + "]");
                            ZKClient.this.connect();
                        } catch (Throwable e) {
                            LOG.warn("Recovery mode: reconnect attempt failed [" + count + "]... waiting for " + _reconnectTimeout, e);
                            try {
                                _lock.wait(_reconnectTimeout.getDurationInMilliseconds());
                            } catch(InterruptedException e1) {
                                throw new RuntimeException("Recovery mode: wait interrupted... bailing out", e1);
                            }
                        }
                    }
                } finally {
                    _expiredSessionRecovery = null;
                    LOG.info("Exiting recovery mode.");
                }
            }
        }
    }

    /**
     * Constructor
     */
    public ZKClient(String connectString, Timespan sessionTimeout, Watcher watcher)
    {
        this(new ZooKeeperFactory(connectString, sessionTimeout, watcher));
    }

    /**
     * Constructor
     */
    public ZKClient(IZooKeeperFactory factory)
    {
        this(factory, null);
    }

    /**
     * Constructor
     */
    public ZKClient(IZooKeeperFactory factory, String chroot)
    {
        super(chroot);
        _factory = factory;
        Map<String, String> acls = new HashMap<String, String>();
        acls.put("/", "world:anyone:acdrw");
        setACLs(acls);
    }

    static private int getPermFromString(String permString) {
        int perm = 0;
        for (int i = 0; i < permString.length(); i++) {
            switch (permString.charAt(i)) {
                case 'r':
                    perm |= ZooDefs.Perms.READ;
                    break;
                case 'w':
                    perm |= ZooDefs.Perms.WRITE;
                    break;
                case 'c':
                    perm |= ZooDefs.Perms.CREATE;
                    break;
                case 'd':
                    perm |= ZooDefs.Perms.DELETE;
                    break;
                case 'a':
                    perm |= ZooDefs.Perms.ADMIN;
                    break;
                default:
                    System.err
                            .println("Unknown perm type: " + permString.charAt(i));
            }
        }
        return perm;
    }

    private static List<ACL> parseACLs(String aclString) {
        List<ACL> acl;
        String acls[] = aclString.split(",");
        acl = new ArrayList<ACL>();
        for (String a : acls) {
            int firstColon = a.indexOf(':');
            int lastColon = a.lastIndexOf(':');
            if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
                System.err
                        .println(a + " does not have the form scheme:id:perm");
                continue;
            }
            ACL newAcl = new ACL();
            newAcl.setId(new Id(a.substring(0, firstColon), a.substring(
                    firstColon + 1, lastColon)));
            newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
            acl.add(newAcl);
        }
        return acl;
    }

    public Stat createOrSetByteWithParents(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws InterruptedException, KeeperException {
        if (exists(path) != null) {
            return setByteData(path, data);
        }
        try {
            createBytesNodeWithParents(path, data, acl, createMode);
            return null;
        } catch(KeeperException.NodeExistsException e) {
            // this should not happen very often (race condition)
            return setByteData(path, data);
        }
    }

    public String create(String path, CreateMode createMode) throws InterruptedException, KeeperException {
        return create(path, (byte[]) null, createMode);
    }

    public String create(String path, String data, CreateMode createMode) throws InterruptedException, KeeperException {
        return create(path, toByteData(data), createMode);
    }

    public String create(String path, byte[] data, CreateMode createMode) throws InterruptedException, KeeperException {
        return getZk().create(adjustPath(path), data, getNodeACLs(path), createMode);
    }

    public String createWithParents(String path, CreateMode createMode) throws InterruptedException, KeeperException {
        return createWithParents(path, (byte[]) null, createMode);
    }

    public String createWithParents(String path, String data, CreateMode createMode) throws InterruptedException, KeeperException {
        return createWithParents(path, toByteData(data), createMode);
    }

    public String createWithParents(String path, byte[] data, CreateMode createMode) throws InterruptedException, KeeperException {
        createParents(path);
        return create(path, data, createMode);
    }

    public Stat createOrSetWithParents(String path, String data, CreateMode createMode) throws InterruptedException, KeeperException {
        return createOrSetWithParents(path, toByteData(data), createMode);
    }

    public Stat createOrSetWithParents(String path, byte[] data, CreateMode createMode) throws InterruptedException, KeeperException {
        if (exists(path) != null) {
            return setByteData(path, data);
        }
        try {
            createWithParents(path, data, createMode);
            return null;
        } catch (KeeperException.NodeExistsException e) {
            // this should not happen very often (race condition)
            return setByteData(path, data);
        }
    }

    public void fixACLs(String path, boolean recursive) throws InterruptedException, KeeperException {
        if (exists(path) != null) {
            doFixACLs(path, recursive);
        }
    }

    private void doFixACLs(String path, boolean recursive) throws KeeperException, InterruptedException {
        setACL(path, getNodeACLs(path), -1);
        if (recursive) {
            for (String child : getChildren(path)) {
                doFixACLs(path.equals("/") ? "/" + child : path + "/" + child, recursive);
            }
        }
    }

    private List<ACL> getNodeACLs(String path) {
        String acl = doGetNodeACLs(adjustPath(path));
        if (acl == null) {
            throw new IllegalStateException("Could not find matching ACLs for " + path);
        }
        return parseACLs(acl);
    }

    protected String doGetNodeACLs(String path) {
        String longestPath = "";
        for (String acl : acls.keySet()) {
            if (acl.length() > longestPath.length() && path.startsWith(acl)) {
                longestPath = acl;
            }
        }
        return acls.get(longestPath);
    }

    private void createParents(String path) throws InterruptedException, KeeperException {
        path = PathUtils.getParentPath(adjustPath(path));
        path = PathUtils.removeTrailingSlash(path);
        List<String> paths = new ArrayList<String>();
        while(!path.equals("") && getZk().exists(path, false) == null) {
            paths.add(path);
            path = PathUtils.getParentPath(path);
            path = PathUtils.removeTrailingSlash(path);
        }
        Collections.reverse(paths);
        for(String p : paths) {
            try {
                getZk().create(p,
                        null,
                        getNodeACLs(p),
                        CreateMode.PERSISTENT);
            } catch(KeeperException.NodeExistsException e) {
                // ok we continue...
                if (LOG.isDebugEnabled()) {
                    LOG.debug("parent already exists " + p);
                }
            }
        }
    }

    private byte[] toByteData(String data) {
        if (data == null) {
            return null;
        } else {
            try {
                return data.getBytes(CHARSET);
            } catch(UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
