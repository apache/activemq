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

import org.apache.activecluster.ClusterEvent;
import org.apache.activecluster.ClusterListener;

/**
 * A {@link ClusterListener} which maintains a {@link GroupModel} implementation
 *
 * @version $Revision: 1.2 $
 */
public class GroupClusterListener implements ClusterListener {
    private GroupModel model;

    public GroupClusterListener(GroupModel model) {
        this.model = model;
    }

    // Properties
    //-------------------------------------------------------------------------
    public GroupModel getModel() {
        return model;
    }

    // ClusterListener interface
    //-------------------------------------------------------------------------
    public void onNodeAdd(ClusterEvent event) {
        model.addNode(event.getNode());
    }

    public void onNodeUpdate(ClusterEvent event) {
    }

    public void onNodeRemoved(ClusterEvent event) {
        model.removeNode(event.getNode());
    }

    public void onNodeFailed(ClusterEvent event) {
        model.removeNode(event.getNode());
    }

    public void onCoordinatorChanged(ClusterEvent event) {
    }
}
