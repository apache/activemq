/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activecluster.group;

import java.util.List;

import org.apache.activecluster.group.BuddyGroupModel;
import org.apache.activecluster.group.Group;
import org.apache.activecluster.group.GroupModel;

/**
 * @version $Revision: 1.4 $
 */
public class BuddyGroupModelTest extends GroupTestSupport {

    public void testGroups() throws Exception {
        addNode("a");

        // lets check how many groups have been created
        List groups = model.getGroups();
        assertEquals("number of groups: " + groups, 1, model.getGroups().size());

        Group group = (Group) model.getGroups().get(0);
        assertIncomplete(group);

        addNode("b");
        assertEquals("number of groups: " + groups, 2, model.getGroups().size());

        // lets see if the first node is now complete
        assertUsable(group);

        group = (Group) model.getGroups().get(1);
        assertUsable(group);


        addNode("c");
        assertEquals("number of groups: " + groups, 3, model.getGroups().size());
        group = (Group) model.getGroups().get(2);
        assertUsable(group);


        addNode("d");
        assertEquals("number of groups: " + groups, 4, model.getGroups().size());
        group = (Group) model.getGroups().get(3);
        assertUsable(group);

    }

    public void testRemoveGroups() {
        String[] nodeNames = {"a", "b", "c"};
        addNodes(nodeNames);

        // TODO now lets remove the nodes and check group states..
    }

    protected GroupModel createGroupModel() {
        return new BuddyGroupModel();
    }
}
