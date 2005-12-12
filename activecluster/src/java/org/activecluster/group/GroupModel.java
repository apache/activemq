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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Represents a collection of zero or more groups in a cluster.
 * The default implementation will create groups as nodes are added to the cluster; filling
 * the groups with its required number of buddies / slaves until a new group can be created.
 * <p/>
 * Nodes which are not allowed to be master nodes will be kept around in a pool ready to be added
 * as slaves when a new master arrives and forces the creation of a group.
 *
 * @version $Revision: 1.2 $
 * @see Group
 */
public class GroupModel {
    private int maximumGroups = -1;
    private int minimumMemberCount = 2;
    private int maximumMemberCount = 3;
    private List groups = new ArrayList();
    private LinkedList incompleteGroups = new LinkedList();
    private LinkedList completeGroups = new LinkedList();
    private LinkedList fullGroups = new LinkedList();
    private LinkedList unusedNodes = new LinkedList();
    private NodeFilter masterFilter;
    private Map nodeMemberships = new HashMap();

    // allow a node to be a master and 2 buddies
    private int maximumWeighting = 10;

    /**
     * Adds the new node to this group model; we assume the node has not been added before.
     *
     * @param node
     */
    public synchronized void addNode(Node node) {
        if (!addToExistingGroup(node)) {
            Group group = makeNewGroup(node);
            if (group == null) {
                addToUnusedNodes(node);
            }
            else {
                addGroup(group);
            }
        }
    }

    /**
     * Removes the node from the group model
     *
     * @param node
     */
    public synchronized void removeNode(Node node) {
        unusedNodes.remove(node);

        // lets remove the node from each group
        for (Iterator iter = groups.iterator(); iter.hasNext();) {
            Group group = (Group) iter.next();
            boolean wasFull = group.isFull();
            boolean wasUsable = group.isUsable();

            if (removeNodeFromGroup(group, node)) {
                updateGroupCollections(group, wasFull, wasUsable);
            }
        }
    }

    // Properties
    //-------------------------------------------------------------------------

    /**
     * Returns a snapshot of the groups currently available
     */
    public synchronized List getGroups() {
        return new ArrayList(groups);
    }

    public NodeFilter getMasterFilter() {
        return masterFilter;
    }

    public void setMasterFilter(NodeFilter masterFilter) {
        this.masterFilter = masterFilter;
    }

    public int getMaximumGroups() {
        return maximumGroups;
    }

    public void setMaximumGroups(int maximumGroups) {
        this.maximumGroups = maximumGroups;
    }

    public int getMaximumMemberCount() {
        return maximumMemberCount;
    }

    public void setMaximumMemberCount(int maximumMemberCount) {
        this.maximumMemberCount = maximumMemberCount;
    }

    public int getMinimumMemberCount() {
        return minimumMemberCount;
    }

    public void setMinimumMemberCount(int minimumMemberCount) {
        this.minimumMemberCount = minimumMemberCount;
    }

    public int getMaximumWeighting() {
        return maximumWeighting;
    }

    public void setMaximumWeighting(int maximumWeighting) {
        this.maximumWeighting = maximumWeighting;
    }


    // Implementation methods
    //-------------------------------------------------------------------------


    /**
     * Attempt to make a new group with the current node as the master
     * or if the node cannot be a master node
     *
     * @return the newly created group or false if none was created.
     */
    protected Group makeNewGroup(Node node) {
        // no pending groups available so lets try and create a new group
        if (canCreateGroup(node)) {
            Group group = createGroup(node);
            addNodeToGroup(group, node);
            return group;
        }
        else {
            return null;
        }
    }

    protected void tryToFillGroupWithBuddies(Group group) {
        boolean continueFillingGroups = true;
        while (!group.isUsable() && continueFillingGroups) {
            continueFillingGroups = tryToAddBuddy(group);
        }

        if (continueFillingGroups) {
            // lets try fill more unfilled nodes
            for (Iterator iter = new ArrayList(incompleteGroups).iterator(); iter.hasNext() && continueFillingGroups;) {
                group = (Group) iter.next();

                boolean wasFull = group.isFull();
                boolean wasUsable = group.isUsable();

                while (!group.isUsable() && continueFillingGroups) {
                    continueFillingGroups = tryToAddBuddy(group);
                }

                if (group.isUsable()) {
                    updateGroupCollections(group, wasFull, wasUsable);
                }
            }
        }
    }

    protected boolean tryToAddBuddy(Group group) {
        boolean continueFillingGroups = true;
        // TODO we could make this much faster using a weighting-sorted collection
        NodeMemberships lowest = null;
        int lowestWeight = 0;
        for (Iterator iter = nodeMemberships.values().iterator(); iter.hasNext();) {
            NodeMemberships memberships = (NodeMemberships) iter.next();
            if (!memberships.isMember(group)) {
                int weighting = memberships.getWeighting();
                if ((lowest == null || weighting < lowestWeight) && weighting < maximumWeighting) {
                    lowest = memberships;
                    lowestWeight = weighting;
                }
            }
        }
        if (lowest == null) {
            continueFillingGroups = false;
        }
        else {
            addNodeToGroup(group, lowest.getNode());
        }
        return continueFillingGroups;
    }

    /**
     * Lets move the group from its current state collection to the new collection if its
     * state has changed
     */
    protected void updateGroupCollections(Group group, boolean wasFull, boolean wasUsable) {
        boolean full = group.isFull();
        if (wasFull && !full) {
            fullGroups.remove(group);
        }
        boolean usable = group.isUsable();
        if (wasUsable && !usable) {
            completeGroups.remove(group);
        }
        if ((!usable || !full) && (wasFull || wasUsable)) {
            incompleteGroups.add(group);
        }
    }

    protected void addToUnusedNodes(Node node) {
        // lets add the node to the pool ready to be used if a node fails
        unusedNodes.add(node);
    }

    /**
     * Attempts to add the node to an incomplete group, or
     * a not-full group and returns true if its possible - else returns false
     *
     * @return true if the node has been added to a groupu
     */
    protected boolean addToExistingGroup(Node node) {
        if (!addToIncompleteGroup(node)) {
            if (!addToNotFullGroup(node)) {
                return false;
            }
        }
        return true;
    }

    protected boolean addToNotFullGroup(Node node) {
        return addToPendingGroup(completeGroups, node);
    }

    protected boolean addToIncompleteGroup(Node node) {
        return addToPendingGroup(incompleteGroups, node);
    }

    /**
     * Adds the given node to the first pending group if possible
     *
     * @return true if the node was added to the first available group
     */
    protected boolean addToPendingGroup(LinkedList list, Node node) {
        if (!list.isEmpty()) {
            Group group = (Group) list.getFirst();
            addNodeToGroup(group, node);
            if (group.isFull()) {
                list.removeFirst();
                fullGroups.add(group);
            }
            else if (group.isUsable()) {
                list.removeFirst();
                completeGroups.add(group);
            }
            return true;
        }
        return false;
    }

    protected void addNodeToGroup(Group group, Node node) {
        NodeMemberships memberships = (NodeMemberships) nodeMemberships.get(node);
        if (memberships == null) {
            memberships = new NodeMemberships(node);
            nodeMemberships.put(node, memberships);
        }
        memberships.addToGroup(group);
    }

    protected boolean removeNodeFromGroup(Group group, Node node) {
        NodeMemberships memberships = (NodeMemberships) nodeMemberships.get(node);
        if (memberships != null) {
            return memberships.removeFromGroup(group);
        }
        return false;
    }


    protected void addGroup(Group group) {
        groups.add(group);
        if (group.isFull()) {
            fullGroups.add(group);
        }
        else if (group.isUsable()) {
            completeGroups.add(group);
        }
        else {
            incompleteGroups.add(group);
        }
    }

    protected Group createGroup(Node node) {
        return new Group(minimumMemberCount, maximumMemberCount);
    }

    /**
     * Returns true if we can add a new group to the cluster
     */
    protected boolean canCreateGroup(Node node) {
        return (maximumGroups < 0 || groups.size() < maximumGroups) && canBeMaster(node);
    }

    /**
     * Returns true if the given node can be a master
     */
    protected boolean canBeMaster(Node node) {
        return masterFilter == null || masterFilter.evaluate(node);
    }
}
