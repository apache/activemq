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

import org.apache.activemq.state.CommandVisitor;


/**
 * 
 * @openwire:marshaller code="3"
 * @version $Revision: 1.11 $
 */
public class ConnectionInfo extends BaseCommand {
    
    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.CONNECTION_INFO;
    
    protected ConnectionId connectionId;
    protected String clientId;
    protected String userName;
    protected String password;
    protected BrokerId[] brokerPath;
    
    public ConnectionInfo() {        
    }    
    public ConnectionInfo(ConnectionId connectionId) {
        this.connectionId=connectionId;
    }
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }
    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }
    
    /**
     * @openwire:property version=1
     */
    public String getClientId() {
        return clientId;
    }
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    
    public RemoveInfo createRemoveCommand() {
        RemoveInfo command = new RemoveInfo(getConnectionId());
        command.setResponseRequired(isResponseRequired());
        return command;
    }

    /**
     * @openwire:property version=1
     */
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @openwire:property version=1
     */
    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * The route of brokers the command has moved through. 
     * 
     * @openwire:property version=1 cache=true
     */
    public BrokerId[] getBrokerPath() {
        return brokerPath;
    }
    public void setBrokerPath(BrokerId[] brokerPath) {
        this.brokerPath = brokerPath;
    }
    
    public Response visit(CommandVisitor visitor) throws Throwable {
        return visitor.processAddConnection( this );
    }

}
