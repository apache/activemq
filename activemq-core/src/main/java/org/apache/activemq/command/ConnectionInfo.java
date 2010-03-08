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
 * @openwire:marshaller code="3"
 * @version $Revision: 1.11 $
 */
public class ConnectionInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.CONNECTION_INFO;

    protected ConnectionId connectionId;
    protected String clientId;
    protected String userName;
    protected String password;
    protected BrokerId[] brokerPath;
    protected boolean brokerMasterConnector;
    protected boolean manageable;
    protected boolean clientMaster = true;
    protected boolean faultTolerant = false;
    protected transient Object transportContext;

    public ConnectionInfo() {
    }

    public ConnectionInfo(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public ConnectionInfo copy() {
        ConnectionInfo copy = new ConnectionInfo();
        copy(copy);
        return copy;
    }

    private void copy(ConnectionInfo copy) {
        super.copy(copy);
        copy.connectionId = connectionId;
        copy.clientId = clientId;
        copy.userName = userName;
        copy.password = password;
        copy.brokerPath = brokerPath;
        copy.brokerMasterConnector = brokerMasterConnector;
        copy.manageable = manageable;
        copy.clientMaster = clientMaster;
        copy.transportContext = transportContext;
        copy.faultTolerant= faultTolerant;
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

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processAddConnection(this);
    }

    /**
     * @openwire:property version=1
     */
    public boolean isBrokerMasterConnector() {
        return brokerMasterConnector;
    }

    /**
     * @param slaveBroker The brokerMasterConnector to set.
     */
    public void setBrokerMasterConnector(boolean slaveBroker) {
        this.brokerMasterConnector = slaveBroker;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isManageable() {
        return manageable;
    }

    /**
     * @param manageable The manageable to set.
     */
    public void setManageable(boolean manageable) {
        this.manageable = manageable;
    }

    /**
     * Transports may wish to associate additional data with the connection. For
     * example, an SSL transport may use this field to attach the client
     * certificates used when the conection was established.
     * 
     * @return the transport context.
     */
    public Object getTransportContext() {
        return transportContext;
    }

    /**
     * Transports may wish to associate additional data with the connection. For
     * example, an SSL transport may use this field to attach the client
     * certificates used when the conection was established.
     * 
     * @param transportContext value used to set the transport context
     */
    public void setTransportContext(Object transportContext) {
        this.transportContext = transportContext;
    }

    /**
     * @openwire:property version=2
     * @return the clientMaster
     */
    public boolean isClientMaster() {
        return this.clientMaster;
    }

    /**
     * @param clientMaster the clientMaster to set
     */
    public void setClientMaster(boolean clientMaster) {
        this.clientMaster = clientMaster;
    }

    /**
     * @openwire:property version=6 cache=false
     * @return the faultTolerant
     */
    public boolean isFaultTolerant() {
        return this.faultTolerant;
    }

    /**
     * @param faultTolerant the faultTolerant to set
     */
    public void setFaultTolerant(boolean faultTolerant) {
        this.faultTolerant = faultTolerant;
    }

}
