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
package org.apache.activecluster;

import java.util.Map;
import javax.jms.Destination;


/**
 * Represents a node member in a cluster
 *
 * @version $Revision: 1.3 $
 */
public interface Node {

    /**
     * Access to the queue to send messages direct to this node.
     *
     * @return the destination to send messages to this node while its available
     */
    public Destination getDestination();

    /**
     * @return an immutable map of the nodes state
     */
    public Map getState();

    /**
     * @return the name of the node
     */
    public String getName();

    /**
     * @return true if this node has been elected as coordinator
     */
    public boolean isCoordinator();

    /**
     * Returns the Zone of this node - typically the DMZ zone or the subnet on which the
     * node is on
     */
    public Object getZone();
}
