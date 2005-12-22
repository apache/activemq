/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activecluster.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import javax.jms.Destination;
import javax.jms.JMSException;
import org.activecluster.Cluster;
import org.activecluster.ClusterEvent;
import org.activecluster.ClusterListener;
import org.activecluster.Node;
import org.activecluster.election.ElectionStrategy;
import org.activecluster.election.impl.BullyElectionStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;


/**
 * Represents a node list
 *
 * @version $Revision: 1.4 $
 */
public class StateServiceImpl implements StateService {

    private final static Log log = LogFactory.getLog(StateServiceImpl.class);
    private Cluster cluster;
    private Object clusterLock;
    private Map nodes = new ConcurrentHashMap();
    private long inactiveTime;
    private List listeners =  new CopyOnWriteArrayList();
    private Destination localDestination;
    private Runnable localNodePing;
    private NodeImpl coordinator;
    private ElectionStrategy electionStrategy;

    /**
     * @param cluster
     * @param clusterLock
     * @param localNodePing
     * @param timer
     * @param inactiveTime
     */
    /**
     * Constructor StateServiceImpl
     * @param cluster
     * @param clusterLock
     * @param localNodePing
     * @param timer
     * @param inactiveTime
     */
    public StateServiceImpl(Cluster cluster, Object clusterLock, Runnable localNodePing, Timer timer, long inactiveTime) {
        this.cluster = cluster;
        this.clusterLock = clusterLock;
        this.localDestination = cluster.getLocalNode().getDestination();
        this.localNodePing = localNodePing;
        this.inactiveTime = inactiveTime;
        long delay = inactiveTime / 3;
        timer.scheduleAtFixedRate(createTimerTask(), delay, delay);
        (this.coordinator = (NodeImpl) cluster.getLocalNode()).setCoordinator(true);
        this.electionStrategy = new BullyElectionStrategy();
    }

    /**
     * @return the current election strategy
     */
    public ElectionStrategy getElectionStrategy() {
        return electionStrategy;
    }

    /**
     * set the election strategy
     *
     * @param electionStrategy
     */
    public void setElectionStrategy(ElectionStrategy electionStrategy) {
        this.electionStrategy = electionStrategy;
    }

    /**
     * Get time of since last communication
     * @return length of time inactive
     */
    public long getInactiveTime() {
        return inactiveTime;
    }

    /**
     * Set the time inactive
     * @param inactiveTime
     */
    public void setInactiveTime(long inactiveTime) {
        this.inactiveTime = inactiveTime;
    }

    /**
     * Get A Map of nodes - where key = destination, value = node
     * @return map of destination/nodes
     */
    public Map getNodes() {
        HashMap answer = new HashMap(nodes.size());
        for (Iterator iter = nodes.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            NodeEntry nodeEntry = (NodeEntry) entry.getValue();
            answer.put(key, nodeEntry.node);
        }
        return answer;
    }

    /**
     * Got a keepalive
     * @param node 
     */
    public void keepAlive(Node node) {
        Object key = node.getDestination();
        if (key != null && !localDestination.equals(key)) {
            NodeEntry entry = (NodeEntry) nodes.get(key);
            if (entry == null) {
                entry = new NodeEntry();
                entry.node = node;
                nodes.put(key, entry);
                nodeAdded(node);
                synchronized (clusterLock) {
                    clusterLock.notifyAll();
                }
            }
            else {
                // has the data changed
                if (stateHasChanged(entry.node, node)) {
                    entry.node = node;
                    nodeUpdated(node);
                }
            }

            // lets update the timer at which the node will be considered
            // to be dead
            entry.lastKeepAlive = getTimeMillis();
        }
    }

    /**
     * shutdown the node
     */
    public void shutdown(Node node){
        Object key=node.getDestination();
        if(key!=null){
            nodes.remove(key);
            ClusterEvent event=new ClusterEvent(cluster,node,ClusterEvent.ADD_NODE);
            for (Iterator i = listeners.iterator(); i.hasNext();){
                ClusterListener listener=(ClusterListener) i.next();
                listener.onNodeRemoved(event);
            }
        }
    }
    
    /**
     * check nodes are alive
     *
     */

    public void checkForTimeouts() {
        localNodePing.run();
        long time = getTimeMillis();
        for (Iterator iter = nodes.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Entry) iter.next();
            NodeEntry nodeEntry = (NodeEntry) entry.getValue();
            if (nodeEntry.lastKeepAlive + inactiveTime < time) {
                iter.remove();
                nodeFailed(nodeEntry.node);
            }
        }
    }

    public TimerTask createTimerTask() {
        return new TimerTask() {
            public void run() {
                checkForTimeouts();
            }
        };
    }

    public void addClusterListener(ClusterListener listener) {
        listeners.add(listener);
    }

    public void removeClusterListener(ClusterListener listener) {
        listeners.remove(listener);
    }

    protected void nodeAdded(Node node) {
        ClusterEvent event = new ClusterEvent(cluster, node, ClusterEvent.ADD_NODE);
        // lets take a copy to make contention easier
        Object[] array = listeners.toArray();
        for (int i = 0, size = array.length; i < size; i++) {
            ClusterListener listener = (ClusterListener) array[i];
            listener.onNodeAdd(event);
        }
        doElection();
    }

    protected void nodeUpdated(Node node) {
        ClusterEvent event = new ClusterEvent(cluster, node, ClusterEvent.UPDATE_NODE);
        // lets take a copy to make contention easier
        Object[] array = listeners.toArray();
        for (int i = 0, size = array.length; i < size; i++) {
            ClusterListener listener = (ClusterListener) array[i];
            listener.onNodeUpdate(event);
        }
    }

    protected void nodeFailed(Node node) {
        ClusterEvent event = new ClusterEvent(cluster, node, ClusterEvent.REMOVE_NODE);
        // lets take a copy to make contention easier
        Object[] array = listeners.toArray();
        for (int i = 0, size = array.length; i < size; i++) {
            ClusterListener listener = (ClusterListener) array[i];
            listener.onNodeFailed(event);
        }
        doElection();
    }

    protected void coordinatorChanged(Node node) {
        ClusterEvent event = new ClusterEvent(cluster, node, ClusterEvent.ELECTED_COORDINATOR);
        // lets take a copy to make contention easier
        Object[] array = listeners.toArray();
        for (int i = 0, size = array.length; i < size; i++) {
            ClusterListener listener = (ClusterListener) array[i];
            listener.onCoordinatorChanged(event);
        }
    }

    protected void doElection() {
        if (electionStrategy != null) {
            try {
                NodeImpl newElected = (NodeImpl) electionStrategy.doElection(cluster);
                if (newElected != null && !newElected.equals(coordinator)) {
                    coordinator.setCoordinator(false);
                    coordinator = newElected;
                    coordinator.setCoordinator(true);
                    coordinatorChanged(coordinator);
                }
            }
            catch (JMSException jmsEx) {
                log.error("do election failed", jmsEx);
            }
        }
    }

    /**
     * For performance we may wish to use a less granualar timing mechanism
     * only updating the time every x millis since we're only using
     * the time as a judge of when a node has not pinged for at least a few
     * hundred millis etc.
     */
    protected long getTimeMillis() {
        return System.currentTimeMillis();
    }

    protected static class NodeEntry {
        public Node node;
        public long lastKeepAlive;
    }


    /**
     * @return true if the node has changed state from the old in memory copy to the
     *         newly arrived copy
     */
    protected boolean stateHasChanged(Node oldNode, Node newNode) {
        Map oldState = oldNode.getState();
        Map newState = newNode.getState();
        if (oldState == newState) {
            return false;
        }
        return oldState == null || newState == null || !oldState.equals(newState);
    }
}
