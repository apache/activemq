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

import java.io.IOException;

import org.apache.activemq.state.CommandVisitor;

/**
 * Used to create and destroy destinations on the broker.
 * 
 * @openwire:marshaller code="8"
 * @version $Revision: 1.9 $
 */
public class DestinationInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.DESTINATION_INFO;

    public static final byte ADD_OPERATION_TYPE = 0;
    public static final byte REMOVE_OPERATION_TYPE = 1;

    protected ConnectionId connectionId;
    protected ActiveMQDestination destination;
    protected byte operationType;
    protected long timeout;
    protected BrokerId[] brokerPath;

    public DestinationInfo() {
    }

    public DestinationInfo(ConnectionId connectionId, byte operationType, ActiveMQDestination destination) {
        this.connectionId = connectionId;
        this.operationType = operationType;
        this.destination = destination;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isAddOperation() {
        return operationType == ADD_OPERATION_TYPE;
    }

    public boolean isRemoveOperation() {
        return operationType == REMOVE_OPERATION_TYPE;
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
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * @openwire:property version=1
     */
    public byte getOperationType() {
        return operationType;
    }

    public void setOperationType(byte operationType) {
        this.operationType = operationType;
    }

    /**
     * @openwire:property version=1
     */
    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
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
        if (isAddOperation()) {
            return visitor.processAddDestination(this);
        } else if (isRemoveOperation()) {
            return visitor.processRemoveDestination(this);
        }
        throw new IOException("Unknown operation type: " + getOperationType());
    }

}
