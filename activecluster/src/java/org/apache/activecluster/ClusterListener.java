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

package org.apache.activecluster;

import java.util.EventListener;


/**
 * Listener to events occuring on the cluster
 *
 * @version $Revision: 1.2 $
 */
public interface ClusterListener extends EventListener {

    /**
     * A new node has been added
     *
     * @param event
     */
    public void onNodeAdd(ClusterEvent event);

    /**
     * A node has updated its state
     *
     * @param event
     */
    public void onNodeUpdate(ClusterEvent event);

    /**
     * A node has been removed (a clean shutdown)
     *
     * @param event
     */
    public void onNodeRemoved(ClusterEvent event);

    /**
     * A node has failed due to process or network failure
     *
     * @param event
     */
    public void onNodeFailed(ClusterEvent event);
    
    /**
     * An election has occurred and a new coordinator has been selected
     * @param event
     */
    public void onCoordinatorChanged(ClusterEvent event);
}
