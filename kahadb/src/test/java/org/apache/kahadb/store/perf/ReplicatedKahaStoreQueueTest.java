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
package org.apache.kahadb.store.perf;

import java.io.File;
import java.util.Arrays;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.perf.SimpleQueueTest;
import org.apache.kahadb.replication.ClusterState;
import org.apache.kahadb.replication.ReplicationService;
import org.apache.kahadb.replication.StaticClusterStateManager;

/**
 * @version $Revision: 712224 $
 */
public class ReplicatedKahaStoreQueueTest extends SimpleQueueTest {

	private StaticClusterStateManager cluster;
	
	private static final String BROKER1_REPLICATION_ID = "kdbr://localhost:60001";
	private static final String BROKER2_REPLICATION_ID = "kdbr://localhost:60002";

    protected String broker2BindAddress="tcp://localhost:61617";
    private ReplicationService rs1;
    private ReplicationService rs2;

	@Override
	protected BrokerService createBroker(String uri) throws Exception {
		
	    clientURI="failover:(" +
	    		"tcp://localhost:61616?wireFormat.cacheEnabled=true&wireFormat.tightEncodingEnabled=true&wireFormat.maxInactivityDuration=50000" +
	    		"," +
	    		"tcp://localhost:61617?wireFormat.cacheEnabled=true&wireFormat.tightEncodingEnabled=true&wireFormat.maxInactivityDuration=50000" +
	    		")?jms.useAsyncSend=true";
	    
		// This cluster object will control who becomes the master.
		cluster = new StaticClusterStateManager();

		ClusterState clusterState = new ClusterState();
		clusterState.setMaster(BROKER1_REPLICATION_ID);
		String[] slaves = {BROKER2_REPLICATION_ID};
		clusterState.setSlaves(Arrays.asList(slaves));
		cluster.setClusterState(clusterState);

        rs1 = new ReplicationService();
        rs1.setUri(BROKER1_REPLICATION_ID);
        rs1.setCluster(cluster);
        rs1.setDirectory(new File("target/replication-test/broker1"));
        rs1.setBrokerURI("broker://("+uri+")/broker1");
        rs1.start();

        rs2 = new ReplicationService();
        rs2.setUri(BROKER2_REPLICATION_ID);
        rs2.setCluster(cluster);
        rs2.setDirectory(new File("target/replication-test/broker2"));
        rs2.setBrokerURI("broker://(" + broker2BindAddress + ")/broker2");
        rs2.start();

		return rs1.getBrokerService();
	}
	
	@Override
	protected void tearDown() throws Exception {
		if( rs1!=null ) {
			rs1.stop();
			rs1 = null;
		}
        if( rs2!=null ) {
            rs2.stop();
            rs2 = null;
        }
	}
	
}

