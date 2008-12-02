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
package org.apache.kahadb.replication.blaze;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activeblaze.cluster.BlazeClusterGroupChannel;
import org.apache.activeblaze.cluster.BlazeClusterGroupChannelFactory;
import org.apache.activeblaze.cluster.BlazeClusterGroupConfiguration;
import org.apache.activeblaze.cluster.MasterChangedListener;
import org.apache.activeblaze.group.Member;
import org.apache.activeblaze.group.MemberChangedListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.replication.ClusterListener;
import org.apache.kahadb.replication.ClusterState;
import org.apache.kahadb.replication.ClusterStateManager;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus;

/**
 * 
 * @author rajdavies
 * @org.apache.xbean.XBean element="blazeCluster"
 */
public class BlazeClusterStateManager extends BlazeClusterGroupConfiguration implements ClusterStateManager,
        MemberChangedListener, MasterChangedListener {
    private static final Log LOG = LogFactory.getLog(BlazeClusterStateManager.class);
    private BlazeClusterGroupChannel channel;
    private AtomicBoolean started = new AtomicBoolean();
    private ClusterState clusterState;
    private String localMemberName;
    private PBClusterNodeStatus status;
    final private List<ClusterListener> listeners = new CopyOnWriteArrayList<ClusterListener>();

    /**
     * @param listener
     * @see org.apache.kahadb.replication.ClusterStateManager#addListener(org.apache.kahadb.replication.ClusterListener)
     */
    public void addListener(ClusterListener listener) {
        this.listeners.add(listener);
        initializeChannel();
        fireClusterChange();
    }

    /**
     * @param node
     * @see org.apache.kahadb.replication.ClusterStateManager#addMember(java.lang.String)
     */
    public void addMember(String node) {
        this.localMemberName = node;
        initializeChannel();
    }

    /**
     * @param listener
     * @see org.apache.kahadb.replication.ClusterStateManager#removeListener(org.apache.kahadb.replication.ClusterListener)
     */
    public void removeListener(ClusterListener listener) {
        this.listeners.remove(listener);
    }

    /**
     * @param node
     * @see org.apache.kahadb.replication.ClusterStateManager#removeMember(java.lang.String)
     */
    public void removeMember(String node) {
    }

    /**
     * @param status
     * @see org.apache.kahadb.replication.ClusterStateManager#setMemberStatus(org.apache.kahadb.replication.pb.PBClusterNodeStatus)
     */
    public void setMemberStatus(PBClusterNodeStatus status) {
        if (status != null) {
            this.status = status;
            setMasterWeight(status.getState().getNumber());
            if (this.channel != null) {
                this.channel.getConfiguration().setMasterWeight(getMasterWeight());
                try {
                    this.channel.waitForElection(getAwaitGroupTimeout());
                } catch (Exception e) {
                    LOG.error("Wait for Election Failed");
                }
            }
            processClusterStateChange();
        }
    }

    /**
     * @param arg0
     * @see org.apache.activeblaze.group.MemberChangedListener#memberStarted(org.apache.activeblaze.group.Member)
     */
    public void memberStarted(Member arg0) {
        processClusterStateChange();
    }

    /**
     * @param arg0
     * @see org.apache.activeblaze.group.MemberChangedListener#memberStopped(org.apache.activeblaze.group.Member)
     */
    public void memberStopped(Member arg0) {
        processClusterStateChange();
    }

    /**
     * @param arg0
     * @see org.apache.activeblaze.cluster.MasterChangedListener#masterChanged(org.apache.activeblaze.group.Member)
     */
    public void masterChanged(Member arg0) {
        processClusterStateChange();
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.Service#start()
     */
    public void start() throws Exception {
        if (this.started.compareAndSet(false, true)) {
            initializeChannel();
        }
        this.started.set(true);
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.Service#stop()
     */
    public void stop() throws Exception {
        if (this.started.compareAndSet(true, false)) {
            if (this.channel != null) {
                this.channel.removeMemberChangedListener(this);
                this.channel.removeMasterChangedListener(this);
                this.channel.shutDown();
                this.channel = null;
            }
        }
        this.started.set(false);
    }

    private boolean isStarted() {
        return this.started.get();
    }

    synchronized private void updateClusterState(ClusterState clusterState) {
        this.clusterState = clusterState;
        fireClusterChange();
    }

    private void fireClusterChange() {
        if (isStarted() && !this.listeners.isEmpty() && this.clusterState != null) {
            for (ClusterListener listener : this.listeners) {
                listener.onClusterChange(this.clusterState);
            }
        }
    }

    private void processClusterStateChange() {
        if (isStarted()) {
            try {
                ClusterState state = new ClusterState();
                this.channel.waitForElection(getAwaitGroupTimeout());
                Set<Member> members = this.channel.getMembers();
                Member master = this.channel.getMaster();
                if (master != null) {
                    // check we can be the master
                    if (!this.channel.isMaster() || (this.status != null)) {
                        state.setMaster(master.getName());
                        members.remove(master);
                    }
                }
                List<String> slaves = new ArrayList<String>();
                for (Member slave : members) {
                    slaves.add(slave.getName());
                }
                state.setSlaves(slaves);
                updateClusterState(state);
            } catch (Exception e) {
                LOG.error("Failed to process Cluster State Changed", e);
            }
        }
    }

    private synchronized void initializeChannel() {
        if (this.localMemberName != null && this.channel == null) {
            try {
                BlazeClusterGroupChannelFactory factory = new BlazeClusterGroupChannelFactory(this);
                this.channel = factory.createChannel(this.localMemberName);
                this.channel.addMemberChangedListener(this);
                this.channel.addMasterChangedListener(this);
                if (isStarted()) {
                    this.channel.start();
                    this.channel.waitForElection(getAwaitGroupTimeout());
                }
                processClusterStateChange();
            } catch (Exception e) {
                LOG.error("Failed to create channel", e);
            }
        }
    }
}
