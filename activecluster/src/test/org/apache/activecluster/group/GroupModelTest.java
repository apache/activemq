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

import java.util.List;
import javax.jms.JMSException;

import org.apache.activecluster.group.Group;

/**
 * @version $Revision: 1.4 $
 */
public class GroupModelTest extends GroupTestSupport {

    public void testGroups() throws Exception {
        addNode("a");

        // lets check how many groups have been created
        List groups = model.getGroups();
        assertEquals("number of groups: " + groups, 1, model.getGroups().size());

        Group group = (Group) model.getGroups().get(0);
        assertIncomplete(group);

        addNode("b");
        assertNotFullButUsable(group);
        assertEquals("number of groups: " + groups, 1, model.getGroups().size());

        addNode("c");
        assertFull(group);
        assertEquals("number of groups: " + groups, 1, model.getGroups().size());


        addNode("d");
        assertEquals("number of groups: " + groups, 2, model.getGroups().size());
        group = (Group) model.getGroups().get(1);
        assertIncomplete(group);
    }

    public void testRemoveGroups() throws JMSException {
        String[] nodeNames = {"a", "b", "c"};
        addNodes(nodeNames);

        // TODO now lets remove the nodes and check group states..
    }

}
