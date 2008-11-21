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

import java.util.ArrayList;

import org.apache.kahadb.replication.pb.PBClusterNodeStatus;

public class StaticClusterStateManager implements ClusterStateManager {

	final private ArrayList<ClusterListener> listeners = new ArrayList<ClusterListener>();
	private ClusterState clusterState;
	private int startCounter;

	synchronized public ClusterState getClusterState() {
		return clusterState;
	}

	synchronized public void setClusterState(ClusterState clusterState) {
		this.clusterState = clusterState;
		fireClusterChange();
	}

	synchronized public void addListener(ClusterListener listener) {
		listeners.add(listener);
		fireClusterChange();
	}

	synchronized public void removeListener(ClusterListener listener) {
		listeners.remove(listener);
	}

	synchronized public void start() throws Exception {
		startCounter++;
		fireClusterChange();
	}

	synchronized private void fireClusterChange() {
		if( startCounter>0 && !listeners.isEmpty() && clusterState!=null ) {
			for (ClusterListener listener : listeners) {
				listener.onClusterChange(clusterState);
			}
		}
	}

	synchronized public void stop() throws Exception {
		startCounter--;
	}

    public void addMember(String node) {
    }

    public void removeMember(String node) {
    }

    public void setMemberStatus(PBClusterNodeStatus status) {
    }

}
