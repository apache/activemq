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
 * Represents a partial command; a large command that has been split up into
 * pieces.
 *
 * @openwire:marshaller code="60"
 *
 */
public class PartialCommand implements Command {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.PARTIAL_COMMAND;

    private int commandId;
    private byte[] data;

    private transient Endpoint from;
    private transient Endpoint to;

    public PartialCommand() {
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1
     */
    @Override
    public int getCommandId() {
        return commandId;
    }

    @Override
    public void setCommandId(int commandId) {
        this.commandId = commandId;
    }

    /**
     * The data for this part of the command
     *
     * @openwire:property version=1 mandatory=true
     */
    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public Endpoint getFrom() {
        return from;
    }

    @Override
    public void setFrom(Endpoint from) {
        this.from = from;
    }

    @Override
    public Endpoint getTo() {
        return to;
    }

    @Override
    public void setTo(Endpoint to) {
        this.to = to;
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        throw new IllegalStateException("The transport layer should filter out PartialCommand instances but received: " + this);
    }

    @Override
    public boolean isResponseRequired() {
        return false;
    }

    @Override
    public boolean isResponse() {
        return false;
    }

    @Override
    public boolean isBrokerInfo() {
        return false;
    }

    @Override
    public boolean isMessageDispatch() {
        return false;
    }

    @Override
    public boolean isMessage() {
        return false;
    }

    @Override
    public boolean isMessageAck() {
        return false;
    }

    @Override
    public boolean isMessageDispatchNotification() {
        return false;
    }

    @Override
    public boolean isShutdownInfo() {
        return false;
    }

    @Override
    public boolean isConnectionControl() {
        return false;
    }

    @Override
    public boolean isConsumerControl() {
        return false;
    }

    @Override
    public void setResponseRequired(boolean responseRequired) {
    }

    @Override
    public boolean isWireFormatInfo() {
        return false;
    }

    @Override
    public boolean isMarshallAware() {
        return false;
    }

    @Override
    public String toString() {
        int size = 0;
        if (data != null) {
            size = data.length;
        }

        return "PartialCommand[id: " + commandId + " data: " + size + " byte(s)]";
    }
}
