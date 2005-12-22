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
package org.activecluster.group;

import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;
import org.activecluster.Cluster;
import org.activecluster.ClusterEvent;
import org.activecluster.ClusterListener;
import org.activecluster.DestinationMarshaller;
import org.activecluster.Node;
import org.activecluster.impl.DefaultDestinationMarshaller;
import org.activecluster.impl.NodeImpl;

/**
 * A base class for Group model testing
 *
 * @version $Revision: 1.5 $
 */
public abstract class GroupTestSupport extends TestCase {

    protected GroupModel model;
    private ClusterListener listener;
    private Cluster cluster;
    private Map nodes = new HashMap();
    private DestinationMarshaller marshaller = new DefaultDestinationMarshaller();

    protected void addNodes(String[] nodeNames) {
        for (int i = 0; i < nodeNames.length; i++) {
            String nodeName = nodeNames[i];
            addNode(nodeName);
        }
    }

    protected void addNode(String nodeName) {
        
        Node node = new NodeImpl(nodeName,marshaller.getDestination(nodeName));
        nodes.put(nodeName, node);
        listener.onNodeAdd(new ClusterEvent(cluster, node, ClusterEvent.ADD_NODE));
    }

    protected void assertFull(Group group) {
        assertTrue("Group is not full and usable. Members: " + group.getMembers(), group.isFull() && group.isUsable());
    }

    protected void assertNotFullButUsable(Group group) {
        assertTrue("Group is not not full but usable. Members: " + group.getMembers(), !group.isFull() && group.isUsable());
    }

    protected void assertIncomplete(Group group) {
        assertTrue("Group is not not full or usable. Members: " + group.getMembers(), !group.isFull() && !group.isUsable());
    }

    protected void assertUsable(Group group) {
        assertTrue("Group is not usable. Members: " + group.getMembers(), group.isUsable());
    }

    protected void setUp() throws Exception {
        model = createGroupModel();
        listener = new GroupClusterListener(model);
    }

    protected GroupModel createGroupModel() {
        return new GroupModel();
    }
}
