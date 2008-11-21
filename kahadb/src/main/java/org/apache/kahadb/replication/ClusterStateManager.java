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

import org.apache.activemq.Service;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus;

/**
 * This interface is used by the ReplicationService to know when
 * it should switch between Slave and Master mode. 
 * 
 * @author chirino
 */
public interface ClusterStateManager extends Service {

    /**
     * Adds a ClusterListener which is used to get notifications
     * of chagnes in the cluster state.
     * @param listener
     */
	void addListener(ClusterListener listener);
	
	/**
	 * Removes a previously added ClusterListener
	 * @param listener
	 */
	void removeListener(ClusterListener listener);

	/**
	 * Adds a member to the cluster.  Adding a member does not mean he is online.
	 * Some ClusterStateManager may keep track of a persistent memebership list
	 * so that can determine if there are enough nodes online to form a quorum
	 * for the purposes of electing a master.
	 * 
	 * @param node
	 */
    public void addMember(final String node);
    
    /**
     * Removes a previously added member.
     * 
     * @param node
     */
    public void removeMember(final String node);

    /**
     * Updates the status of the local node.
     * 
     * @param status
     */
    public void setMemberStatus(final PBClusterNodeStatus status);
}
