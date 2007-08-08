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
 * A ProducerAck command is sent by a broker to a producer to let it know it has
 * received and processed messages that it has produced. The producer will be
 * flow controlled if it does not receive ProducerAck commands back from the
 * broker.
 * 
 * @openwire:marshaller code="19" version="3"
 * @version $Revision: 1.11 $
 */
public class ProducerAck extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.PRODUCER_ACK;

    protected ProducerId producerId;
    protected int size;

    public ProducerAck() {
    }

    public ProducerAck(ProducerId producerId, int size) {
        this.producerId = producerId;
        this.size = size;
    }

    public void copy(ProducerAck copy) {
        super.copy(copy);
        copy.producerId = producerId;
        copy.size = size;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processProducerAck(this);
    }

    /**
     * The producer id that this ack message is destined for.
     * 
     * @openwire:property version=3
     */
    public ProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(ProducerId producerId) {
        this.producerId = producerId;
    }

    /**
     * The number of bytes that are being acked.
     * 
     * @openwire:property version=3
     */
    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

}
