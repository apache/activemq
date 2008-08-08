/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.group;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.jms.Destination;
import org.apache.activemq.util.IdGenerator;
import com.sun.jndi.url.corbaname.corbanameURLContextFactory;

/**
 *<P>
 * A <CODE>Member</CODE> holds information about a member of the group
 * 
 */
public class Member implements Externalizable {
    private String name;
    private String id;
    private String hostname;
    private long startTime;
    private int coordinatorWeight;
    private Destination inBoxDestination;
    private transient long timeStamp;
    

    /**
     * Default constructor - only used by serialization
     */
    public Member() {    
    }
    /**
     * @param name
     */
    public Member(String name) {
        this.name = name;
        this.hostname = IdGenerator.getHostName();
        this.startTime=System.currentTimeMillis();
    }

    /**
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the id
     */
    public String getId() {
        return this.id;
    }
    
    void setId(String id) {
        this.id=id;
    }
    
    /**
     * @return the hostname
     */
    public String getHostname() {
        return this.hostname;
    }
    
    /**
     * @return the startTime
     */
    public long getStartTime() {
        return this.startTime;
    }
    
    /**
     * @return the inbox destination
     */
    public Destination getInBoxDestination() {
        return this.inBoxDestination;
    }
    
    void setInBoxDestination(Destination dest) {
        this.inBoxDestination=dest;
    }
    
    /**
     * @return the timeStamp
     */
    long getTimeStamp() {
        return this.timeStamp;
    }

    /**
     * @param timeStamp the timeStamp to set
     */
    void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
    /**
     * @return the coordinatorWeight
     */
    public int getCoordinatorWeight() {
        return this.coordinatorWeight;
    }
    /**
     * @param coordinatorWeight the coordinatorWeight to set
     */
    public void setCoordinatorWeight(int coordinatorWeight) {
        this.coordinatorWeight = coordinatorWeight;
    }
    
    
    
    public String toString() {
        return this.name+"["+this.id+"]@"+this.hostname;
    }
    
     
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.coordinatorWeight=in.readInt();;
        this.name = in.readUTF();
        this.id = in.readUTF();
        this.hostname = in.readUTF();
        this.startTime=in.readLong();
        this.inBoxDestination=(Destination) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.coordinatorWeight);
        out.writeUTF(this.name != null ? this.name : "");
        out.writeUTF(this.id != null ? this.id : "");
        out.writeUTF(this.hostname != null ? this.hostname : "");
        out.writeLong(this.startTime);
        out.writeObject(this.inBoxDestination);
    }
    
    public int hashCode() {
        return this.id.hashCode();
    }
    
    public boolean equals(Object obj) {
        boolean result = false;
        if (obj instanceof Member) {
            Member other = (Member)obj;
            result = this.id.equals(other.id);
        }
        return result;
    }
}
