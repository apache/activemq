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
package org.activecluster;

import java.util.List;
import java.util.Map;
import javax.jms.Destination;
import javax.jms.Message;

/**
 * @version $Revision: 1.4 $
 */
public class ClusterTest extends ClusterTestSupport {

    protected int count = 2;

    public void testCluster() throws Exception {
        cluster = createCluster();

        subscribeToCluster();

        cluster.start();

        Destination destination = cluster.getDestination();
        Message message = cluster.createTextMessage("abcdef");
        cluster.send(destination, message);

       //clusterListener.waitForMessageToArrive();
       Thread.sleep(5000);

        List list = clusterListener.flushMessages();
        assertEquals("Should have received a message: " + list, 1, list.size());

        System.out.println("Received message: " + list.get(0));
    }


    public void testMembershipCluster() throws Exception {
        Cluster[] clusters = new Cluster[count];
        for (int i = 0; i < count; i++) {
            Cluster cluster = createCluster("node:" + i);
            clusters[i] = cluster;
            if (i==0){
            cluster.addClusterListener(new TestingClusterListener(cluster));
            }
            cluster.start();
            System.out.println("started " + clusters[i].getLocalNode().getName());

        }

        System.out.println("waiting to complete ...");
        for (int i = count - 1; i >= 0; i--) {
            Cluster cluster = clusters[i];
            String localName = cluster.getLocalNode().getName();
            boolean completed = cluster.waitForClusterToComplete(count - 1, 5000);
            assertTrue("Node: " + i + " with contents: " + dumpConnectedNodes(cluster.getNodes()), completed);

            System.out.println(localName + " completed = " + completed + " nodes = "
                    + dumpConnectedNodes(cluster.getNodes()));
        }

        assertClusterMembership(clusters);

        // lets wait for a while to see if things fail
        Thread.sleep(10000L);

        assertClusterMembership(clusters);

        Cluster testCluster = clusters[0];
        LocalNode testNode = testCluster.getLocalNode();
        String key = "key";
        String value = "value";

        Map map = testNode.getState();
        map.put(key, value);
        testNode.setState(map);

        Thread.sleep(5000);
        for (int i = 1; i < count; i++) {
            Node node = (Node) clusters[i].getNodes().get(testNode.getDestination());
            assertTrue("The current test node should be in the cluster: " + i, node != null);
            assertTrue(node.getState().get(key).equals(value));
        }

        for (int i = 0; i < count; i++) {
            System.out.println(clusters[i].getLocalNode().getName() + " Is coordinator = " + clusters[i].getLocalNode().isCoordinator());
            clusters[i].stop();
            Thread.sleep(250);

        }
    }

    protected void assertClusterMembership(Cluster[] clusters) {
        for (int i = 0; i < count; i++) {
            System.out.println("Cluster: " + i + " = " + clusters[i].getNodes());

            assertEquals("Size of clusters for cluster: " + i, count - 1, clusters[i].getNodes().size());
            System.out.println(clusters[i].getLocalNode().getName() + " Is coordinator = " + clusters[i].getLocalNode().isCoordinator());
        }
    }

}
