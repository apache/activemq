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
 * 
 **/
package org.apache.activecluster.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import org.apache.activecluster.DestinationMarshaller;
import org.apache.activecluster.Node;
/**
 * Default implementation of a remote Node
 * 
 * @version $Revision: 1.3 $
 */
public class NodeState implements Externalizable{
    private static final long serialVersionUID=-3909792803360045064L;
    private String name;
    private String destinationName;
    protected Map state;
    protected boolean coordinator;
    
    /**
     * DefaultConstructor
     *
     */
    public NodeState(){
    }
    
    /**
     * Construct a NodeState from a Node
     * @param node
     * @param marshaller
     */
    public NodeState(Node node, DestinationMarshaller marshaller){
        this.name = node.getName();
        this.destinationName = marshaller.getDestinationName(node.getDestination());
        this.state = node.getState();
        this.coordinator = node.isCoordinator();
    }

    /**
     * @return pretty print of the node
     */
    public String toString(){
        return "NodeState[<"+name+">destinationName: "+destinationName+" state: "+state+"]";
    }

    /**
     * @return Returns the coordinator.
     */
    public boolean isCoordinator(){
        return coordinator;
    }

    /**
     * @param coordinator
     *            The coordinator to set.
     */
    public void setCoordinator(boolean coordinator){
        this.coordinator=coordinator;
    }

    /**
     * @return Returns the destinationName.
     */
    public String getDestinationName(){
        return destinationName;
    }

    /**
     * @param destinationName
     *            The destinationName to set.
     */
    public void setDestinationName(String destinationName){
        this.destinationName=destinationName;
    }

    /**
     * @return Returns the name.
     */
    public String getName(){
        return name;
    }

    /**
     * @param name
     *            The name to set.
     */
    public void setName(String name){
        this.name=name;
    }

    /**
     * @return Returns the state.
     */
    public Map getState(){
        return state;
    }

    /**
     * @param state
     *            The state to set.
     */
    public void setState(Map state){
        this.state=state;
    }

    /**
     * write to a stream
     * 
     * @param out
     * @throws IOException
     */
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeUTF((name!=null?name:""));
        out.writeUTF((destinationName!=null?destinationName:""));
        out.writeBoolean(coordinator);
        out.writeObject(state);
    }

    /**
     * read from a stream
     * 
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException{
        this.name=in.readUTF();
        this.destinationName=in.readUTF();
        this.coordinator=in.readBoolean();
        this.state=(Map) in.readObject();
    }
}
