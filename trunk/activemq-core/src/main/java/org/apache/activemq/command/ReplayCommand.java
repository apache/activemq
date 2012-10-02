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
 * A general purpose replay command for some kind of producer where ranges of
 * messages are asked to be replayed. This command is typically used over a
 * non-reliable transport such as UDP or multicast but could also be used on
 * TCP/IP if a socket has been re-established.
 * 
 * @openwire:marshaller code="65"
 * 
 */
public class ReplayCommand extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.REPLAY;

    private String producerId;
    private int firstAckNumber;
    private int lastAckNumber;
    private int firstNakNumber;
    private int lastNakNumber;

    public ReplayCommand() {
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String getProducerId() {
        return producerId;
    }

    /**
     * Is used to uniquely identify the producer of the sequence
     * 
     * @openwire:property version=1 cache=false
     */
    public void setProducerId(String producerId) {
        this.producerId = producerId;
    }

    public int getFirstAckNumber() {
        return firstAckNumber;
    }

    /**
     * Is used to specify the first sequence number being acknowledged as delivered on the transport
     * so that it can be removed from cache
     * 
     * @openwire:property version=1
     */
    public void setFirstAckNumber(int firstSequenceNumber) {
        this.firstAckNumber = firstSequenceNumber;
    }

    public int getLastAckNumber() {
        return lastAckNumber;
    }

    /**
     * Is used to specify the last sequence number being acknowledged as delivered on the transport
     * so that it can be removed from cache
     * 
     * @openwire:property version=1
     */
    public void setLastAckNumber(int lastSequenceNumber) {
        this.lastAckNumber = lastSequenceNumber;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return null;
    }

    /**
     * Is used to specify the first sequence number to be replayed
     * 
     * @openwire:property version=1
     */
    public int getFirstNakNumber() {
        return firstNakNumber;
    }

    public void setFirstNakNumber(int firstNakNumber) {
        this.firstNakNumber = firstNakNumber;
    }

    /**
     * Is used to specify the last sequence number to be replayed
     * 
     * @openwire:property version=1
     */
    public int getLastNakNumber() {
        return lastNakNumber;
    }

    public void setLastNakNumber(int lastNakNumber) {
        this.lastNakNumber = lastNakNumber;
    }

    public String toString() {
        return "ReplayCommand {commandId = " + getCommandId() + ", firstNakNumber = " + getFirstNakNumber() + ", lastNakNumber = " + getLastNakNumber() + "}";
    }
    
}
