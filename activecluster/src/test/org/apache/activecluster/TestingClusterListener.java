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
 */
package org.apache.activecluster;

import org.apache.activecluster.Cluster;
import org.apache.activecluster.ClusterEvent;
import org.apache.activecluster.ClusterListener;
import org.apache.activecluster.impl.DefaultCluster;

/**
 * @version $Revision: 1.2 $
 */
public class TestingClusterListener implements ClusterListener {
    private Cluster cluster;

    public TestingClusterListener(Cluster cluster){
        this.cluster = cluster;
    }
    public void onNodeAdd(ClusterEvent event) {
            printEvent("ADDED: ", event);
        }

        public void onNodeUpdate(ClusterEvent event) {
            printEvent("UPDATED: ", event);
        }

        public void onNodeRemoved(ClusterEvent event) {
            printEvent("REMOVED: ", event);
        }

        public void onNodeFailed(ClusterEvent event) {
            printEvent("FAILED: ", event);
        }

        public void onCoordinatorChanged(ClusterEvent event) {
            printEvent("COORDINATOR: ", event);
        }

        protected void printEvent(String text, ClusterEvent event) {
            System.out.println(text + event.getNode());
            System.out.println("Current cluster is now: " + cluster.getNodes().keySet());
        }
    }
