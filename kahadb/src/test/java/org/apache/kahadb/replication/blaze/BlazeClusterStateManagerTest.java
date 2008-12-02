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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;
import org.apache.activemq.util.Callback;
import org.apache.kahadb.replication.ClusterListener;
import org.apache.kahadb.replication.ClusterState;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus;
import org.apache.kahadb.replication.pb.PBJournalLocation;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus.State;

/**
 * @author rajdavies
 * 
 */
public class BlazeClusterStateManagerTest extends TestCase {
    public void testTwoNodesGoingOnline() throws Exception {
        final LinkedBlockingQueue<ClusterState> stateEvents1 = new LinkedBlockingQueue<ClusterState>();
        final LinkedBlockingQueue<ClusterState> stateEvents2 = new LinkedBlockingQueue<ClusterState>();
        final BlazeClusterStateManager bcsm1 = new BlazeClusterStateManager();
        bcsm1.addListener(new ClusterListener() {
            public void onClusterChange(ClusterState config) {
                stateEvents1.add(config);
            }
        });
        bcsm1.start();
        bcsm1.addMember("kdbr://localhost:60001");
        final BlazeClusterStateManager bcsm2 = new BlazeClusterStateManager();
        bcsm2.addListener(new ClusterListener() {
            public void onClusterChange(ClusterState config) {
                stateEvents2.add(config);
            }
        });
        bcsm2.start();
        bcsm2.addMember("kdbr://localhost:60002");
        // Drain the events..
        while (stateEvents1.poll(100, TimeUnit.MILLISECONDS) != null) {
        }
        while (stateEvents2.poll(100, TimeUnit.MILLISECONDS) != null) {
        }
        // Bring node 1 online
        final PBClusterNodeStatus status1 = new PBClusterNodeStatus();
        status1.setConnectUri("kdbr://localhost:60001");
        status1.setLastUpdate(new PBJournalLocation().setFileId(1).setOffset(50));
        status1.setState(State.SLAVE_UNCONNECTED);
        executeAsync(new Callback() {
            public void execute() throws Exception {
                bcsm1.setMemberStatus(status1);
            }
        });
        // Bring node 2 online
        final PBClusterNodeStatus status2 = new PBClusterNodeStatus();
        status2.setConnectUri("kdbr://localhost:60002");
        status2.setLastUpdate(new PBJournalLocation().setFileId(2).setOffset(20));
        status2.setState(State.SLAVE_UNCONNECTED);
        executeAsync(new Callback() {
            public void execute() throws Exception {
                Thread.sleep(1000);
                bcsm2.setMemberStatus(status2);
            }
        });
        ClusterState state = stateEvents1.poll(10, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.getMaster());
        assertEquals("kdbr://localhost:60002", state.getMaster());
        assertTrue(state.getSlaves().size() == 1);
        state = stateEvents2.poll(2, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.getMaster());
        assertEquals("kdbr://localhost:60002", state.getMaster());
        assertTrue(state.getSlaves().size() == 1);
        bcsm2.stop();
        bcsm1.stop();
    }

    public void testOneNodeGoingOnline() throws Exception {
        final LinkedBlockingQueue<ClusterState> stateEvents1 = new LinkedBlockingQueue<ClusterState>();
        final BlazeClusterStateManager bcsm1 = new BlazeClusterStateManager();
        bcsm1.addListener(new ClusterListener() {
            public void onClusterChange(ClusterState config) {
                stateEvents1.add(config);
            }
        });
        bcsm1.start();
        // Drain the events..
        while (stateEvents1.poll(100, TimeUnit.MILLISECONDS) != null) {
        }
        // Let node1 join the cluster.
        bcsm1.addMember("kdbr://localhost:60001");
        ClusterState state = stateEvents1.poll(1, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNull(state.getMaster());
        assertTrue(state.getSlaves().size() == 1);
        // Let the cluster know that node1 is online..
        PBClusterNodeStatus status = new PBClusterNodeStatus();
        status.setConnectUri("kdbr://localhost:60001");
        status.setLastUpdate(new PBJournalLocation().setFileId(0).setOffset(0));
        status.setState(State.SLAVE_UNCONNECTED);
        bcsm1.setMemberStatus(status);
        state = stateEvents1.poll(10, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.getMaster());
        assertEquals("kdbr://localhost:60001", state.getMaster());
        assertTrue(state.getSlaves().isEmpty());
        bcsm1.stop();
    }

    private void executeAsync(final Callback callback) {
        new Thread("Async Test Task") {
            @Override
            public void run() {
                try {
                    callback.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }
}
