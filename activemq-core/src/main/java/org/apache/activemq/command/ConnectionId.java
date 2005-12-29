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
public class ConnectionId implements DataStructure {
    
    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.CONNECTION_ID;
    
    protected String connectionId;
    
    public ConnectionId() {        
    }
    
    public ConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }
    
    public ConnectionId(ConnectionId id) {
        this.connectionId = id.getConnectionId();
    }

    public ConnectionId(SessionId id) {
        this.connectionId = id.getConnectionId();
    }

    public ConnectionId(ProducerId id) {
        this.connectionId = id.getConnectionId();
    }
    
    public ConnectionId(ConsumerId id) {
        this.connectionId = id.getConnectionId();
    }

    public int hashCode() {
        return connectionId.hashCode();
    }
    
    public boolean equals(Object o) {
        if( this == o )
            return true;
        if( o == null || o.getClass()!=ConnectionId.class )
            return false;
        ConnectionId id = (ConnectionId) o;
        return connectionId.equals(id.connectionId);
    }
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String toString() {
        return connectionId;
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
    
    public boolean isMarshallAware() {
        return false;
    }
}
