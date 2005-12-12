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
package org.activecluster.impl;

import java.util.HashMap;
import java.util.Map;
import org.activecluster.Node;


/**
 * Default implementation of a remote Node
 *
 * @version $Revision: 1.3 $
 */
public class NodeImpl implements Node {
    private static final long serialVersionUID=-3909792803360045064L;
    private String name;
    private String destination;
    protected Map state;
    protected boolean coordinator;

    /**
     * Allow a node to be copied for sending it as a message
     *
     * @param node
     */
    public NodeImpl(Node node) {
        this(node.getName(),node.getDestination(), node.getState());
    }

    /**
     * Create a Node
     * @param name 
     * @param destination
     */
    public NodeImpl(String name,String destination) {
        this(name,destination, new HashMap());
    }

    /**
     * Create A Node
     * @param name
     * @param destination
     * @param state
     */
    public NodeImpl(String name,String destination, Map state) {
        this.name = name;
        this.destination = destination;
        this.state = state;
    }

    /**
     * @return the name of the node
     */
    public String getName() {
        return name;
    }

    /**
     * @return pretty print of the node
     */
    public String toString() {
        return "Node[<" + name + ">destination: " + destination + " state: " + state + "]";
    }

    /**
     * @return the destination of the node
     */
    public String getDestination() {
        return destination;
    }

    /**
     * Get the State
     * @return the State of the Node
     */
    public synchronized Map getState() {
        return new HashMap(state);
    }


    /**
     * @return true if this node has been elected as coordinator
     */
    public boolean isCoordinator() {
        return coordinator;
    }

    /**
     * Get the zone
     * @return the Zone
     */
    public Object getZone() {
        return state.get("zone");
    }
    
    // Implementation methods
    //-------------------------------------------------------------------------

    protected synchronized void setState(Map state) {
        this.state = state;
    }

    protected void setCoordinator(boolean value) {
        coordinator = value;
    }
}
