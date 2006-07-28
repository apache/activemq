/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
package org.apache.activecluster.group;

import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSException;
import junit.framework.TestCase;
import org.apache.activecluster.Cluster;
import org.apache.activecluster.ClusterEvent;
import org.apache.activecluster.ClusterListener;
import org.apache.activecluster.DestinationMarshaller;
import org.apache.activecluster.Node;
import org.apache.activecluster.impl.NodeImpl;
import org.apache.activecluster.impl.SimpleDestinationMarshaller;

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
    private DestinationMarshaller marshaller = new SimpleDestinationMarshaller();

    protected void addNodes(String[] nodeNames) throws JMSException {
        for (int i = 0; i < nodeNames.length; i++) {
            String nodeName = nodeNames[i];
            addNode(nodeName);
        }
    }

    protected void addNode(String nodeName) throws JMSException {
        
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
