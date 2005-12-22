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
package org.activecluster.group;

import org.activecluster.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a logical group of nodes in a cluster,
 * such as a Master and a number of Slaves which operate as a
 * logical unit.
 * <p/>
 * A cluster can be divided into a single group, or many groups
 * depending on the policy required.
 * <p/>
 * The number of groups could be application defined; created on demand
 * or there could even be one group for each node, with other nodes acting
 * as buddy nodes in each nodes' group (i.e. each node is a master with N
 * buddies/slaves)
 *
 * @version $Revision: 1.2 $
 */
public class Group {
    private int minimumMemberCount;
    private int maximumMemberCount;
    private List members = new ArrayList();
    private int memberCount;

    public Group() {
    }

    public Group(int minimumMemberCount, int maximumMemberCount) {
        this.minimumMemberCount = minimumMemberCount;
        this.maximumMemberCount = maximumMemberCount;
    }

    public synchronized List getMembers() {
        return new ArrayList(members);
    }

    /**
     * Adds a node to the given group
     *
     * @return the index of the node in the group (0 = master, 1..N = slave)
     */
    public synchronized int addMember(Node node) {
        int index = members.indexOf(node);
        if (index >= 0) {
            return index;
        }
        members.add(node);
        return memberCount++;
    }

    public synchronized boolean removeMember(Node node) {
        boolean answer = members.remove(node);
        if (answer) {
            memberCount--;
        }
        return answer;
    }


    /**
     * Returns true if the group is usable, that it has enough members to be used.
     */
    public boolean isUsable() {
        return memberCount >= minimumMemberCount;
    }

    /**
     * Returns true if the group cannot accept any more new members
     */
    public boolean isFull() {
        return memberCount >= maximumMemberCount;
    }

    public int getMemberCount() {
        return memberCount;
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
}
