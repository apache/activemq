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
package org.apache.kahadb.replication.zk;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.replication.ClusterListener;
import org.apache.kahadb.replication.ClusterState;
import org.apache.kahadb.replication.ClusterStateManager;
import org.apache.kahadb.replication.ReplicationSupport;
import org.apache.kahadb.replication.pb.PBClusterConfiguration;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * @author chirino
 * @org.apache.xbean.XBean element="zookeeper-cluster"
 */
public class ZooKeeperClusterStateManager implements ClusterStateManager, Watcher {
    private static final Log LOG = LogFactory.getLog(ZooKeeperClusterStateManager.class);

    final private ArrayList<ClusterListener> listeners = new ArrayList<ClusterListener>();
    private int startCounter;

    private String uri = "zk://localhost:2181/activemq/ha-cluster/default";
    String userid = "activemq";
    String password = "";

    private ZooKeeper zk;
    private String path;

    private ClusterState clusterState;
    private String statusPath;
    private PBClusterNodeStatus memberStatus;
    private Thread takoverTask;
    private boolean areWeTheBestMaster;

    synchronized public void addListener(ClusterListener listener) {
        listeners.add(listener);
        fireClusterChange();
    }

    synchronized public void removeListener(ClusterListener listener) {
        listeners.remove(listener);
    }

    synchronized private void updateClusterState(ClusterState clusterState) {
        this.clusterState = clusterState;
        fireClusterChange();
    }

    synchronized private void fireClusterChange() {
        if (startCounter > 0 && !listeners.isEmpty()) {
            for (ClusterListener listener : listeners) {
                listener.onClusterChange(clusterState);
            }
        }
    }

    synchronized public void start() throws Exception {
        startCounter++;
        if (startCounter == 1) {

            // Make sure the path is set..
            String path = getPath();

            // Create a ZooKeeper connection..
            zk = createZooKeeperConnection();

            while( isStarted() ) {
                try {
                    mkParentDirs(path);
                    try {
                        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (NodeExistsException ignore) {
                    }
                    processClusterStateChange();
                    return;
                } catch (Exception e) {
                    handleZKError(e);
                }
            }
        }
    }
    
    synchronized private boolean isStarted() {
        return startCounter > 0;
    }

    synchronized public void stop() throws Exception {
        startCounter--;
        if (startCounter == 0) {
            zk.close();
            zk = null;
            path=null;
            clusterState=null;
            statusPath=null;
            memberStatus=null;
            takoverTask=null;
            areWeTheBestMaster=false;
            
        }
    }


    public String getPath() {
        if( path == null ) {
            try {
                URI uri = new URI(this.uri);
                path = uri.getPath();
                if (path == null) {
                    throw new IllegalArgumentException("Invalid uri '" + uri + "', path to cluster configuration not specified");
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid uri '" + uri + "': "+e);
            }
        }
        return path;
    }

    ZooKeeper createZooKeeperConnection() throws URISyntaxException, IOException {
        // Parse out the configuration URI.
        URI uri = new URI(this.uri);
        if (!uri.getScheme().equals("zk")) {
            throw new IllegalArgumentException("Invalid uri '" + uri + "', expected it to start with zk://");
        }
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid uri '" + uri + "', host not specified");
        }
        int port = uri.getPort();
        if (port == -1) {
            port = 2181;
        }

        ZooKeeper zk = new ZooKeeper(host, port, this);
        zk.addAuthInfo("digest", (userid+":"+password).getBytes());
        return zk;
    }

    private void processClusterStateChange() {
        try {
            if( zk==null ) {
                return;
            }

            byte[] data = zk.getData(path, new Watcher() {
                public void process(WatchedEvent event) {
                    processClusterStateChange();
                }
            }, new Stat());
            PBClusterConfiguration config = new PBClusterConfiguration();
            config.mergeUnframed(data);
            
            ClusterState state = new ClusterState();
            HashSet<String> slaves = new HashSet<String>(config.getMembersList());
            if( config.hasMaster() ) {
                state.setMaster(config.getMaster());
                slaves.remove(config.getMaster());
            }
            state.setSlaves(new ArrayList<String>(slaves));
            updateClusterState(state);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        System.out.println("Got: " + event);
    }

    public void setMemberStatus(final PBClusterNodeStatus status) {
        while( isStarted() ) {
            try {
                
                this.memberStatus = status;
                if (statusPath == null) {
                    mkdirs(path + "/election");
                    statusPath = zk.create(path + "/election/n_", status.toUnframedByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                } else {
                    Stat stat = zk.exists(statusPath, false);
                    if (status == null) {
                        zk.delete(statusPath, stat.getVersion());
                        statusPath = null;
                    } else {
                        zk.setData(statusPath, status.toUnframedByteArray(), stat.getVersion());
                    }
                }
                processElectionChange();
                return;
                
            } catch (Exception e) {
                e.printStackTrace();
                handleZKError(e);
            }
        }
    }

    synchronized private void processElectionChange() {
        try {
            if( zk==null ) {
                return;
            }
            List<String> zkNodes = zk.getChildren(path + "/election", new Watcher() {
                public void process(WatchedEvent event) {
                    processElectionChange();
                }
            });
            Map<String, PBClusterNodeStatus> children = processNodeStatus(zkNodes);
            
            if( children.isEmpty() ) {
                return;
            }            
            String firstNodeId = children.keySet().iterator().next();
            
            // If we are the first child?
            if( firstNodeId.equals(statusPath) ) {
                
                // If we are master already no need to do anything else
                if ( memberStatus.getConnectUri().equals(clusterState.getMaster()) ) {
                    return;
                }
            
                // We may need to wait till a little to figure out if we are
                // actually the best pick to be the master.
                switch (memberStatus.getState()) {
                case MASTER:                        
                case SLAVE_ONLINE:
                    // Can transition to master immediately
                    LOG.info("Online salve taking over as master.");
                        setMaster(memberStatus.getConnectUri());
                        return;
   
                    case SLAVE_SYNCRONIZING:
                    case SLAVE_UNCONNECTED:                        
                        
                        // If it looks like we are the best master.. lets wait 5 secs to
                    // let other slaves
                    // join the cluster and get a chance to take over..
                    if (areWeTheBestMaster(children)) {
                        
                        areWeTheBestMaster = true;
                        if( takoverTask==null ) {
                            LOG.info(memberStatus.getConnectUri()+" looks like the best offline slave that can take over as master.. waiting 5 secs to allow another slave to take over.");
                            
                            takoverTask = new Thread("Slave takeover..") {
                                public void run() {
                                    takoverAttempt();
                                }
                            };
                            takoverTask.setDaemon(true);
                            takoverTask.start();
                        }
                        return;
                        
                    } else {
                        if( areWeTheBestMaster ) {
                            LOG.info(memberStatus.getConnectUri()+" no longer looks like the best offline slave that can take over as master.");
                        }
                        
                        areWeTheBestMaster = false;
                        
                        // If we get here we need to yield our top position in the node
                        // sequence list so that the better
                        // slave can become the master.
                        Stat stat = zk.exists(statusPath, false);
                        zk.delete(statusPath, stat.getVersion());
                        statusPath = zk.create(path + "/election/n_", memberStatus.toUnframedByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void takoverAttempt() {
        try {
            for( int i=0; i < 10; i++ ) {
                Thread.sleep(500);
                if( !isStarted() )
                    return;
            }
            
            synchronized(this) {
                try {
                    if( areWeTheBestMaster ) {
                        LOG.info(memberStatus.getConnectUri()+" is taking over as master.");
                        setMaster(memberStatus.getConnectUri());
                    }
                } finally {
                    // We want to make sure we set takoverTask to null in the same mutex as we set the master.
                    takoverTask=null;
                }
            }
        } catch (Exception e) {
        } finally {
            // sleep might error out..
            synchronized(this) {
                takoverTask=null;
            }
        }
    }

    private boolean areWeTheBestMaster(Map<String, PBClusterNodeStatus> children) {
        Location ourLocation = ReplicationSupport.convert(memberStatus.getLastUpdate());
        for (Entry<String, PBClusterNodeStatus> entry : children.entrySet()) {
            PBClusterNodeStatus status = entry.getValue();
            switch (status.getState()) {
            case MASTER:
            case SLAVE_ONLINE:
                return false;

            case SLAVE_SYNCRONIZING:
            case SLAVE_UNCONNECTED:
                if (ourLocation.compareTo(ReplicationSupport.convert(status.getLastUpdate())) < 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private Map<String, PBClusterNodeStatus> processNodeStatus(List<String> children) throws KeeperException, InterruptedException, InvalidProtocolBufferException {
        java.util.TreeMap<String, PBClusterNodeStatus> rc = new java.util.TreeMap<String, PBClusterNodeStatus>();
        for (String nodeId : children) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData(path + "/election/" + nodeId, false, stat);
                PBClusterNodeStatus status = new PBClusterNodeStatus();
                status.mergeUnframed(data);
                rc.put(path + "/election/" + nodeId, status);
            } catch (NoNodeException ignore) {
            }
        }
        return rc;
    }

    public void addMember(final String node) {
        while( isStarted() ) {
            try {
                mkParentDirs(path);
                update(path, CreateMode.PERSISTENT, new Updater<InvalidProtocolBufferException>() {
                    public byte[] update(byte[] data) throws InvalidProtocolBufferException {
                        PBClusterConfiguration config = new PBClusterConfiguration();
                        if (data != null) {
                            config.mergeUnframed(data);
                        }
                        if (!config.getMembersList().contains(node)) {
                            config.addMembers(node);
                        }
                        return config.toFramedByteArray();
                    }
                });
                return;
            } catch (Exception e) {
                handleZKError(e);
            }
        }
    }

    public void removeMember(final String node) {
        while( isStarted() ) {
            try {
                mkParentDirs(path);
                update(path, CreateMode.PERSISTENT, new Updater<InvalidProtocolBufferException>() {
                    public byte[] update(byte[] data) throws InvalidProtocolBufferException {
                        PBClusterConfiguration config = new PBClusterConfiguration();
                        if (data != null) {
                            config.mergeUnframed(data);
                        }
                        config.getMembersList().remove(node);
                        return config.toFramedByteArray();
                    }
                });
                return;
            } catch (Exception e) {
                handleZKError(e);
            }
        }
    }

    public void setMaster(final String node) throws InvalidProtocolBufferException, KeeperException, InterruptedException {
        mkParentDirs(path);
        update(path, CreateMode.PERSISTENT, new Updater<InvalidProtocolBufferException>() {
            public byte[] update(byte[] data) throws InvalidProtocolBufferException {
                PBClusterConfiguration config = new PBClusterConfiguration();
                if (data != null) {
                    config.mergeUnframed(data);
                }
                config.setMaster(node);
                return config.toFramedByteArray();
            }
        });
    }

    interface Updater<T extends Throwable> {
        byte[] update(byte[] data) throws T;
    }

    private <T extends Throwable> void update(String path, CreateMode persistent, Updater<T> updater) throws InvalidProtocolBufferException, KeeperException, InterruptedException, T {
        Stat stat = zk.exists(path, false);
        if (stat != null) {
            byte[] data = zk.getData(path, false, stat);
            data = updater.update(data);
            zk.setData(path, data, stat.getVersion());
        } else {
            byte[] update = updater.update(null);
            try {
                zk.create(path, update, Ids.OPEN_ACL_UNSAFE, persistent);
            } catch (NodeExistsException ignore) {
                stat = zk.exists(path, false);
                byte[] data = zk.getData(path, false, stat);
                data = updater.update(data);
                zk.setData(path, data, stat.getVersion());
            }
        }
    }

    private void mkParentDirs(String path) throws KeeperException, InterruptedException {
        int lastIndexOf = path.lastIndexOf("/");
        if (lastIndexOf >= 0) {
            mkdirs(path.substring(0, lastIndexOf));
        }
    }

    private void mkdirs(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) != null) {
            return;
        }
        // Remove the leading /
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        String[] split = path.split("/");
        String cur = "";
        for (String node : split) {
            cur += "/" + node;
            try {
                zk.create(cur, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (NodeExistsException ignore) {
            }
        }
    }

    
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    
    private void handleZKError(Exception e) {
        LOG.warn("ZooKeeper error.  Will retry operation in 1 seconds");
        LOG.debug("The error was: "+e, e);
        
        for( int i=0; i < 10; i ++) { 
            try {
                if( !isStarted() )
                    return;
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                return;
            }
        }
    }

}
