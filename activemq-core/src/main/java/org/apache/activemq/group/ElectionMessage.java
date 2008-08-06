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

/**
 * Used to pass information around
 *
 */
public class ElectionMessage implements Externalizable{
    static enum MessageType{ELECTION,ANSWER,COORDINATOR};
    private Member member;
    private MessageType type;
    
    /**
     * @return the member
     */
    public Member getMember() {
        return this.member;
    }

    /**
     * @param member the member to set
     */
    public void setMember(Member member) {
        this.member = member;
    }

    /**
     * @return the type
     */
    public MessageType getType() {
        return this.type;
    }

    /**
     * @param type the type to set
     */
    public void setType(MessageType type) {
        this.type = type;
    }
    
    /**
     * @return true if election message
     */
    public boolean isElection() {
        return this.type != null && this.type.equals(MessageType.ELECTION);
    }
    
    /**
     * @return true if answer message
     */
    public boolean isAnswer() {
        return this.type != null && this.type.equals(MessageType.ANSWER);
    }
    
    /**
     * @return true if coordinator message
     */
    public boolean isCoordinator() {
        return this.type != null && this.type.equals(MessageType.COORDINATOR);
    }
    
        
    public ElectionMessage copy() {
        ElectionMessage result = new ElectionMessage();
        result.member=this.member;
        result.type=this.type;
        return result;
    }
    
    
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.member=(Member) in.readObject();
        this.type=(MessageType) in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.member);
        out.writeObject(this.type);
    }
    
    public String toString() {
        return "ElectionMessage: "+ this.member + "{"+this.type+ "}";
    }
}
