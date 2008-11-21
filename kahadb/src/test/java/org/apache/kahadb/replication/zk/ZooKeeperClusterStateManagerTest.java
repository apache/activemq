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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.activemq.util.Callback;
import org.apache.kahadb.replication.ClusterListener;
import org.apache.kahadb.replication.ClusterState;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus;
import org.apache.kahadb.replication.pb.PBJournalLocation;
import org.apache.kahadb.replication.pb.PBClusterNodeStatus.State;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.persistence.FileTxnLog;

public class ZooKeeperClusterStateManagerTest extends TestCase {

    private static final int PORT = 2181;
    private ZooKeeperClusterStateManager zkcsm1;
    private ZooKeeper zk;
    private Factory serverFactory;

    @Override
    protected void setUp() throws Exception {

        ServerStats.registerAsConcrete();
        File tmpDir = new File("target/test-data/zookeeper");
        tmpDir.mkdirs();

        // Reduces startup time..
        System.setProperty("zookeeper.preAllocSize", "100");
        FileTxnLog.setPreallocSize(100);

        ZooKeeperServer zs = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        
        serverFactory = new NIOServerCnxn.Factory(PORT);
        serverFactory.startup(zs);

        zkcsm1 = new ZooKeeperClusterStateManager();
        zk = zkcsm1.createZooKeeperConnection();
        // Cleanup after previous run...
        zkRecusiveDelete(zkcsm1.getPath());
    }
    
    private void zkRecusiveDelete(String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if( stat!=null ) {
            if( stat.getNumChildren() > 0 ) {
                List<String> children = zk.getChildren(path, false);
                for (String node : children) {
                    zkRecusiveDelete(path+"/"+node);
                }
            }
            zk.delete(path, stat.getVersion());
        }
    }

    @Override
    protected void tearDown() throws Exception {
        zk.close();
        serverFactory.shutdown();
        ServerStats.unregister();
    }
    
    public void testTwoNodesGoingOnline() throws Exception {
        final LinkedBlockingQueue<ClusterState> stateEvents1 = new LinkedBlockingQueue<ClusterState>();
        final LinkedBlockingQueue<ClusterState> stateEvents2 = new LinkedBlockingQueue<ClusterState>();
        
        zkcsm1.addListener(new ClusterListener() {
            public void onClusterChange(ClusterState config) {
                stateEvents1.add(config);
            }
        });
        zkcsm1.start();
        zkcsm1.addMember("kdbr://localhost:60001");
        
        final ZooKeeperClusterStateManager zkcsm2 = new ZooKeeperClusterStateManager();
        zkcsm2.addListener(new ClusterListener() {
            public void onClusterChange(ClusterState config) {
                stateEvents2.add(config);
            }
        });
        zkcsm2.start();
        zkcsm2.addMember("kdbr://localhost:60002");
        
        // Drain the events..
        while( stateEvents1.poll(100, TimeUnit.MILLISECONDS)!=null ) {
        }
        while( stateEvents2.poll(100, TimeUnit.MILLISECONDS)!=null ) {
        }
        
        // Bring node 1 online
        final PBClusterNodeStatus status1 = new PBClusterNodeStatus();
        status1.setConnectUri("kdbr://localhost:60001");
        status1.setLastUpdate(new PBJournalLocation().setFileId(1).setOffset(50));
        status1.setState(State.SLAVE_UNCONNECTED);

        executeAsync(new Callback() {
            public void execute() throws Exception {
                zkcsm1.setMemberStatus(status1);
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
                zkcsm2.setMemberStatus(status2);
            }
         });

        ClusterState state = stateEvents1.poll(10, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.getMaster());
        assertEquals("kdbr://localhost:60002", state.getMaster());
        assertTrue(state.getSlaves().size()==1);

        state = stateEvents2.poll(1, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.getMaster());
        assertEquals("kdbr://localhost:60002", state.getMaster());
        assertTrue(state.getSlaves().size()==1);

        zkcsm2.stop();
        zkcsm1.stop();
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
    
    public void testOneNodeGoingOnline() throws Exception {
        final LinkedBlockingQueue<ClusterState> stateEvents1 = new LinkedBlockingQueue<ClusterState>();
        zkcsm1.addListener(new ClusterListener() {
            public void onClusterChange(ClusterState config) {
                stateEvents1.add(config);
            }
        });
        zkcsm1.start();
        
        // Drain the events..
        while( stateEvents1.poll(100, TimeUnit.MILLISECONDS)!=null ) {
        }
        
        // Let node1 join the cluster.
        zkcsm1.addMember("kdbr://localhost:60001");

        ClusterState state = stateEvents1.poll(1, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNull(state.getMaster());
        assertTrue(state.getSlaves().size()==1);
        
        // Let the cluster know that node1 is online..
        PBClusterNodeStatus status = new PBClusterNodeStatus();
        status.setConnectUri("kdbr://localhost:60001");
        status.setLastUpdate(new PBJournalLocation().setFileId(0).setOffset(0));
        status.setState(State.SLAVE_UNCONNECTED);
        zkcsm1.setMemberStatus(status);

        state = stateEvents1.poll(10, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.getMaster());
        assertEquals("kdbr://localhost:60001", state.getMaster());
        assertTrue(state.getSlaves().isEmpty());

        zkcsm1.stop();
    }
}
