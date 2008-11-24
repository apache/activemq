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
package org.apache.kahadb.replication;

import java.io.File;
import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus.State;
import org.apache.kahadb.store.KahaDBStore;

/**
 * Handles interfacing with the ClusterStateManager and handles activating the
 * slave or master facets of the broker.
 * 
 * @author chirino
 * @org.apache.xbean.XBean element="kahadbReplication"
 */
public class ReplicationService implements Service, ClusterListener {

    private static final String JOURNAL_PREFIX = "journal-";

    private static final Log LOG = LogFactory.getLog(ReplicationService.class);

    private String brokerURI = "xbean:broker.xml";
    private File directory = new File(IOHelper.getDefaultDataDirectory());
    private File tempReplicationDir;
    private String uri;
    private ClusterStateManager cluster;
    private int minimumReplicas=1;
    
    private KahaDBStore store;

    private ClusterState clusterState;
    private BrokerService brokerService;
    private ReplicationMaster master;
    private ReplicationSlave slave;

    public void start() throws Exception {
        if( cluster==null ) {
            throw new IllegalArgumentException("The cluster field has not been set.");
        }
        // The cluster will let us know about the cluster configuration,
        // which lets us decide if we are going to be a slave or a master.
        getStore().open();
        cluster.addListener(this);
        cluster.start();
        
        cluster.addMember(getUri());
        cluster.setMemberStatus(createStatus(State.SLAVE_UNCONNECTED));
    }

    public PBClusterNodeStatus createStatus(State state) throws IOException {
        final PBClusterNodeStatus status = new PBClusterNodeStatus();
        status.setConnectUri(getUri());
        status.setLastUpdate(ReplicationSupport.convert(getStore().getLastUpdatePosition()));
        status.setState(state);
        return status;
    }

    public void stop() throws Exception {
        cluster.removeListener(this);
        cluster.stop();
        stopMaster();
        stopSlave();
        getStore().close();
    }

    public void onClusterChange(ClusterState clusterState) {
        this.clusterState = clusterState;
        try {
            synchronized (cluster) {
                if (areWeTheSlave(clusterState)) {
                    // If we were the master we need to stop the master
                    // service..
                    stopMaster();
                    // If the slave service was not yet started.. start it up.
                    if( clusterState.getMaster()==null ) {
                        stopSlave();
                    } else {
                        startSlave();
                        slave.onClusterChange(clusterState);
                    }
                } else if (areWeTheMaster(clusterState)) {
                    // If we were the slave we need to stop the slave service..
                    stopSlave();
                    // If the master service was not yet started.. start it up.
                    startMaster();
                    master.onClusterChange(clusterState);
                } else {
                    // We were not part of the configuration (not master nor
                    // slave).
                    // So we have to shutdown any running master or slave
                    // services that may
                    // have been running.
                    stopMaster();
                    stopSlave();
                    getCluster().setMemberStatus(createStatus(State.SLAVE_UNCONNECTED));

                }
            }
        } catch (Exception e) {
            LOG.warn("Unexpected Error: " + e, e);
        }
    }

    private void startMaster() throws IOException, Exception {
        if (master == null) {
            LOG.info("Starting replication master.");
            getCluster().setMemberStatus(createStatus(State.MASTER));
            brokerService = createBrokerService();
            brokerService.start();
            master = new ReplicationMaster(this);
            master.start();
        }
    }

    private void stopSlave() throws Exception {
        if (slave != null) {
            LOG.info("Stopping replication slave.");
            slave.stop();
            slave = null;
        }
    }

    private void startSlave() throws Exception {
        if (slave == null) {
            LOG.info("Starting replication slave.");
            slave = new ReplicationSlave(this);
            slave.start();
        }
    }

    private void stopMaster() throws Exception, IOException {
        if (master != null) {
            LOG.info("Stopping replication master.");
            master.stop();
            master = null;
            brokerService.stop();
            brokerService = null;
            // Stopping the broker service actually stops the store
            // too..
            // so we need to open it back up.
            getStore().open();
        }
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    private BrokerService createBrokerService() throws Exception {
        BrokerService rc = BrokerFactory.createBroker(brokerURI);
        rc.setPersistenceAdapter(getStore());
        return rc;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    private boolean areWeTheSlave(ClusterState config) {
        return config.getSlaves().contains(uri);
    }

    private boolean areWeTheMaster(ClusterState config) {
        return uri.equals(config.getMaster());
    }
    
    ///////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////

    public File getReplicationFile(String fn) throws IOException {
        if (fn.equals("database")) {
            return getStore().getPageFile().getFile();
        }
        if (fn.startsWith(JOURNAL_PREFIX)) {
            int id;
            try {
                id = Integer.parseInt(fn.substring(JOURNAL_PREFIX.length()));
            } catch (NumberFormatException e) {
                throw new IOException("Unknown replication file name: " + fn);
            }
            return getStore().getJournal().getFile(id);
        } else {
            throw new IOException("Unknown replication file name: " + fn);
        }
    }


    public File getTempReplicationFile(String fn, int snapshotId) throws IOException {
        if (fn.equals("database")) {
            return new File(getTempReplicationDir(), "database-" + snapshotId);
        }
        if (fn.startsWith(JOURNAL_PREFIX)) {
            int id;
            try {
                id = Integer.parseInt(fn.substring(JOURNAL_PREFIX.length()));
            } catch (NumberFormatException e) {
                throw new IOException("Unknown replication file name: " + fn);
            }
            return new File(getTempReplicationDir(), fn);
        } else {
            throw new IOException("Unknown replication file name: " + fn);
        }
    }

    public boolean isMaster() {
        return master != null;
    }

    public File getTempReplicationDir() {
        if (tempReplicationDir == null) {
            tempReplicationDir = new File(getStore().getDirectory(), "replication");
        }
        return tempReplicationDir;
    }
    public void setTempReplicationDir(File tempReplicationDir) {
        this.tempReplicationDir = tempReplicationDir;
    }

    public KahaDBStore getStore() {
        if (store == null) {
            store = new KahaDBStore();
            store.setDirectory(directory);
        }
        return store;
    }
    public void setStore(KahaDBStore store) {
        this.store = store;
    }

    public File getDirectory() {
        return directory;
    }
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public String getBrokerURI() {
        return brokerURI;
    }
    public void setBrokerURI(String brokerURI) {
        this.brokerURI = brokerURI;
    }

    public String getUri() {
        return uri;
    }
    public void setUri(String nodeId) {
        this.uri = nodeId;
    }
    
    public ClusterStateManager getCluster() {
        return cluster;
    }
    public void setCluster(ClusterStateManager cluster) {
        this.cluster = cluster;
    }

    public int getMinimumReplicas() {
        return minimumReplicas;
    }

    public void setMinimumReplicas(int minimumReplicas) {
        this.minimumReplicas = minimumReplicas;
    }



}
