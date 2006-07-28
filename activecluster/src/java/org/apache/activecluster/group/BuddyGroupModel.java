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
 */
package org.apache.activecluster.group;

import org.apache.activecluster.Node;

/**
 * A kind of {@link GroupModel} in which every {@link Node} has its
 * own {@link Group} and other nodes in the cluster act as buddies (slaves)
 *
 * @version $Revision: 1.2 $
 */
public class BuddyGroupModel extends GroupModel {

    public synchronized void addNode(Node node) {
        Group group = makeNewGroup(node);
        if (group == null) {
            if (!addToExistingGroup(node)) {
                addToUnusedNodes(node);
            }
        }
        else {
            // now lets try choose some existing nodes to add as buddy's
            tryToFillGroupWithBuddies(group);
            
            // now that the group may well be filled, add it to the collections
            addGroup(group);
        }
    }

}
