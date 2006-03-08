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
 * A general purpose replay command for some kind of producer where ranges of
 * messages are asked to be replayed. This command is typically used over a
 * non-reliable transport such as UDP or multicast but could also be used on
 * TCP/IP if a socket has been re-established.
 * 
 * @openwire:marshaller code="38"
 * @version $Revision$
 */
public class ReplayCommand extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.REPLAY;

    private String producerId;
    private long firstSequenceNumber;
    private long lastSequenceNumber;

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

    public long getFirstSequenceNumber() {
        return firstSequenceNumber;
    }

    /**
     * Is used to specify the first sequence number to be replayed
     * 
     * @openwire:property version=1
     */
    public void setFirstSequenceNumber(long firstSequenceNumber) {
        this.firstSequenceNumber = firstSequenceNumber;
    }

    public long getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    /**
     * Is used to specify the last sequence number to be replayed
     * 
     * @openwire:property version=1
     */
    public void setLastSequenceNumber(long lastSequenceNumber) {
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return null;
    }

}
