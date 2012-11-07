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
 * Removes a consumer, producer, session or connection.
 * 
 * @openwire:marshaller code="12"
 * 
 */
public class RemoveInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.REMOVE_INFO;

    protected DataStructure objectId;
    protected long lastDeliveredSequenceId;

    public RemoveInfo() {
    }

    public RemoveInfo(DataStructure objectId) {
        this.objectId = objectId;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public DataStructure getObjectId() {
        return objectId;
    }

    public void setObjectId(DataStructure objectId) {
        this.objectId = objectId;
    }

    /**
     * @openwire:property version=5 cache=false
     */
    public long getLastDeliveredSequenceId() {
        return lastDeliveredSequenceId;
    }

    public void setLastDeliveredSequenceId(long lastDeliveredSequenceId) {
        this.lastDeliveredSequenceId = lastDeliveredSequenceId;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        switch (objectId.getDataStructureType()) {
        case ConnectionId.DATA_STRUCTURE_TYPE:
            return visitor.processRemoveConnection((ConnectionId)objectId, lastDeliveredSequenceId);
        case SessionId.DATA_STRUCTURE_TYPE:
            return visitor.processRemoveSession((SessionId)objectId, lastDeliveredSequenceId);
        case ConsumerId.DATA_STRUCTURE_TYPE:
            return visitor.processRemoveConsumer((ConsumerId)objectId, lastDeliveredSequenceId);
        case ProducerId.DATA_STRUCTURE_TYPE:
            return visitor.processRemoveProducer((ProducerId)objectId);
        default:
            throw new IOException("Unknown remove command type: " + objectId.getDataStructureType());
        }
    }

    /**
     * Returns true if this event is for a removed connection
     */
    public boolean isConnectionRemove() {
        return objectId.getDataStructureType() == ConnectionId.DATA_STRUCTURE_TYPE;
    }

    /**
     * Returns true if this event is for a removed session
     */
    public boolean isSessionRemove() {
        return objectId.getDataStructureType() == SessionId.DATA_STRUCTURE_TYPE;
    }

    /**
     * Returns true if this event is for a removed consumer
     */
    public boolean isConsumerRemove() {
        return objectId.getDataStructureType() == ConsumerId.DATA_STRUCTURE_TYPE;
    }

    /**
     * Returns true if this event is for a removed producer
     */
    public boolean isProducerRemove() {
        return objectId.getDataStructureType() == ProducerId.DATA_STRUCTURE_TYPE;
    }

}
