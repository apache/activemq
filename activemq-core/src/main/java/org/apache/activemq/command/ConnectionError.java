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
package org.apache.activemq.command;

import org.apache.activemq.state.CommandVisitor;


/**
 * 
 * @openwire:marshaller code="16"
 * @version $Revision$
 */
public class ConnectionError extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.CONNECTION_ERROR;

    protected ConnectionId connectionId;
    Throwable exception;
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }
       
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processConnectionError(this);
    }

    /**
     * @openwire:property version=1
     */    
    public Throwable getException() {
        return exception;
    }
    
    public void setException(Throwable exception) {
        this.exception = exception;
    }

    /**
     * @openwire:property version=1
     */    
    public ConnectionId getConnectionId() {
        return connectionId;
    }
    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }
    
}
