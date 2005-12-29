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

package org.apache.activecluster;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A cluster event
 * 
 * @version $Revision: 1.3 $
 */
public class ClusterEvent implements Externalizable {
    
    private static final long serialVersionUID=-4103732679231950873L;
    /**
     * A node has joined the cluster
     */
    public static final int ADD_NODE = 1;
    /**
     * existing node has updated it's state
     */
    public static final int UPDATE_NODE = 2;
    /**
     * A node has left the Cluster
     */
    public static final int REMOVE_NODE = 3;
    /**
     * A node has failed due to a system/network error
     */
    public static final int FAILED_NODE = 4;
    
    /**
     * this node has been elected Coordinator
     */
    public static final int ELECTED_COORDINATOR = 5;
    
    private transient Cluster cluster;
    private Node node;
    private int type;

    /**
     * empty constructor
     */
    public ClusterEvent() {
    }

    /**
     * @param source
     * @param node
     * @param type
     */
    public ClusterEvent(Cluster source, Node node, int type) {
        this.cluster = source;
        this.node = node;
        this.type = type;
    }

    /**
     * @return the Cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * set the cluster
     * @param source
     */
    public void setCluster(Cluster source){
        this.cluster = source;
    }
    /**
     * @return the node
     */
    public Node getNode() {
        return node;
    }

    /**
     * @return the type of event
     */
    public int getType() {
        return type;
    }

    /**
     * @return pretty type
     */
    public String toString() {
        return "ClusterEvent[" + getTypeAsString() + " : " + node + "]";
    }

    /**
     * dump on to a stream
     * 
     * @param out
     * @throws IOException
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
        out.writeObject(node);
    }

    /**
     * read from stream
     * 
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = in.readByte();
        node = (Node) in.readObject();
    }

    private String getTypeAsString() {
        String result = "unknown type";
        if (type == ADD_NODE) {
            result = "ADD_NODE";
        }
        else if (type == REMOVE_NODE) {
            result = "REMOVE_NODE";
        }
        else if (type == UPDATE_NODE) {
            result = "UPDATE_NODE";
        }
        else if (type == FAILED_NODE) {
            result = "FAILED_NODE";
        }
        return result;
    }
}