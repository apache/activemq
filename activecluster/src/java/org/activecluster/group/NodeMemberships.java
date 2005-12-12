/** 
 * 
 * Copyright 2005 LogicBlaze, Inc. (http://www.logicblaze.com)
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

import org.activecluster.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents all of the memberhips of a node and can be used to act
 * as a weighting to decide which is the least heavily loaded Node
 * to be assigned to a buddy group.
 *
 * @version $Revision: 1.2 $
 */
public class NodeMemberships {
    private Node node;
    private Map memberships = new HashMap();
    private int weighting;

    public NodeMemberships(Node node) {
        this.node = node;
    }

    public void addToGroup(Group group) {
        if (!isMember(group)) {
            int index = group.addMember(node);
            Membership membership = new Membership(group, index);
            memberships.put(group, membership);
            weighting += membership.getWeighting();
        }
    }

    public boolean removeFromGroup(Group group) {
        // TODO when we remove a node from a group, we need to reweight the
        // other nodes in the group

        memberships.remove(group);
        return group.removeMember(node);
    }

    public Node getNode() {
        return node;
    }

    /**
     * Returns the weighting of how heavily loaded the node is
     * so that a decision can be made on which node to buddy group
     * with
     */
    public int getWeighting() {
        return weighting;
    }

    /**
     * Returns true if this node is a member of the given group
     */
    public boolean isMember(Group group) {
        return memberships.containsKey(group);
    }
}
