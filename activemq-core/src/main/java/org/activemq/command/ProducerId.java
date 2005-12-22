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
 */
package org.activemq.command;

/**
 * 
 * @openwire:marshaller
 * @version $Revision$
 */
public class ProducerId implements DataStructure {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.PRODUCER_ID;

    protected String connectionId;
    protected long sessionId;
    protected long producerId;

    protected transient int hashCode;
    protected transient String key;
    protected transient SessionId parentId;
    
    public ProducerId() {
    }   
    
    public ProducerId(SessionId sessionId, long producerId) {
        this.connectionId = sessionId.getConnectionId();
        this.sessionId = sessionId.getSessionId();
        this.producerId=producerId;
    }

    public ProducerId(ProducerId id) {
        this.connectionId = id.getConnectionId();
        this.sessionId = id.getSessionId();
        this.producerId=id.getProducerId();
    }

    public ProducerId(String producerKey) {
        // Parse off the producerId
        int p = producerKey.lastIndexOf(":");
        if( p >= 0 ) {
            producerId = Long.parseLong(producerKey.substring(p+1));
            producerKey = producerKey.substring(0,p);
        }
        setProducerSessionKey(producerKey);
    }
    
    public SessionId getParentId() {
        if( parentId == null ) {
            parentId = new SessionId(this);
        }
        return parentId;
    }

    public int hashCode() {
        if( hashCode == 0 ) {
            hashCode = connectionId.hashCode() ^ (int)sessionId ^ (int)producerId;
        }
        return hashCode;
    }
    
    public boolean equals(Object o) {
        if( this == o )
            return true;
        if( o == null || o.getClass()!=ProducerId.class )
            return false;
        ProducerId id = (ProducerId) o;
        return sessionId==id.sessionId 
               && producerId==id.producerId
               && connectionId.equals(id.connectionId);
    }

    
    /**
     * @param sessionKey
     */
    private void setProducerSessionKey(String sessionKey) {
        // Parse off the sessionId
        int p = sessionKey.lastIndexOf(":");
        if( p >= 0 ) {
            sessionId = Long.parseLong(sessionKey.substring(p+1));
            sessionKey = sessionKey.substring(0,p);
        }        
        // The rest is the connectionId
        connectionId = sessionKey;
    }

    public String toString() {
        if( key == null ) {
            key=connectionId+":"+sessionId+":"+producerId;
        }
        return key;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }
    /**
     * @openwire:property version=1 cache=true
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
    public long getProducerId() {
        return producerId;
    }
    public void setProducerId(long producerId) {
        this.producerId = producerId;
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

    public boolean isMarshallAware() {
        return false;
    }
}
