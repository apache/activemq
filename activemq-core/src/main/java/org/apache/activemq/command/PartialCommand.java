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
 * @version $Revision$
 */
public class PartialCommand implements Command {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.PARTIAL_COMMAND;

    private int commandId;
    private byte[] data;

    private transient Endpoint from;
    private transient Endpoint to;

    public PartialCommand() {
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1
     */
    public int getCommandId() {
        return commandId;
    }

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

    public Endpoint getFrom() {
        return from;
    }

    public void setFrom(Endpoint from) {
        this.from = from;
    }

    public Endpoint getTo() {
        return to;
    }

    public void setTo(Endpoint to) {
        this.to = to;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        throw new IllegalStateException("The transport layer should filter out PartialCommand instances but received: " + this);
    }

    public boolean isResponseRequired() {
        return false;
    }

    public boolean isResponse() {
        return false;
    }

    public boolean isBrokerInfo() {
        return false;
    }

    public boolean isMessageDispatch() {
        return false;
    }

    public boolean isMessage() {
        return false;
    }

    public boolean isMessageAck() {
        return false;
    }

    public boolean isMessageDispatchNotification() {
        return false;
    }

    public boolean isShutdownInfo() {
        return false;
    }

    public void setResponseRequired(boolean responseRequired) {
    }

    public boolean isWireFormatInfo() {
        return false;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public String toString() {
        int size = 0;
        if (data != null) {
            size = data.length;
        }
        return "PartialCommand[id: " + commandId + " data: " + size + " byte(s)]";
    }
    
    
}
