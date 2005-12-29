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
package org.apache.activemq.command;


/**
 * 
 * @openwire:marshaller
 * @version $Revision$
 */
public class ConsumerId implements DataStructure {
    
    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.CONSUMER_ID;
    
    protected String connectionId;
    protected long sessionId;
    protected long consumerId;

    protected transient int hashCode;
    protected transient String key;
    protected transient SessionId parentId;
    
    public ConsumerId() {        
    }
    
    public ConsumerId(SessionId sessionId, long consumerId) {
        this.connectionId = sessionId.getConnectionId();
        this.sessionId = sessionId.getSessionId();
        this.consumerId=consumerId;
    }
    
    public ConsumerId(ConsumerId id) {
        this.connectionId = id.getConnectionId();
        this.sessionId = id.getSessionId();
        this.consumerId=id.getConsumerId();
    }
    
    public SessionId getParentId() {
        if( parentId == null ) {
            parentId = new SessionId(this);
        }
        return parentId;
    }

    public int hashCode() {
        if( hashCode == 0 ) {
            hashCode = connectionId.hashCode() ^ (int)sessionId ^ (int)consumerId;
        }
        return hashCode;
    }
    
    public boolean equals(Object o) {
        if( this == o )
            return true;
        if( o == null || o.getClass()!=ConsumerId.class )
            return false;
        ConsumerId id = (ConsumerId) o;
        return sessionId==id.sessionId 
               && consumerId==id.consumerId
               && connectionId.equals(id.connectionId);
    }
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String toString() {
        if( key==null ) {
            key = connectionId+":"+sessionId+":"+consumerId;
        }
        return key;
    }

    /**
     * @openwire:property version=1
     */
    public String getConnectionId() {
        return connectionId;
    }
    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }
    
    /**
     * @openwire:property version=1
     */
    public long getSessionId() {
        return sessionId;
    }    
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }
    

    /**
     * @openwire:property version=1
     */
    public long getConsumerId() {
        return consumerId;
    }
    public void setConsumerId(long consumerId) {
        this.consumerId = consumerId;
    }

    public boolean isMarshallAware() {
        return false;
    }
}
