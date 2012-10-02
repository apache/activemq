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
import org.apache.activemq.util.IntrospectionSupport;

/**
 * @openwire:marshaller code="10"
 * 
 */
public class KeepAliveInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.KEEP_ALIVE_INFO;

    private transient Endpoint from;
    private transient Endpoint to;

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isResponse() {
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

    public boolean isBrokerInfo() {
        return false;
    }

    public boolean isWireFormatInfo() {
        return false;
    }

    /**
     * The endpoint within the transport where this message came from.
     */
    public Endpoint getFrom() {
        return from;
    }

    public void setFrom(Endpoint from) {
        this.from = from;
    }

    /**
     * The endpoint within the transport where this message is going to - null
     * means all endpoints.
     */
    public Endpoint getTo() {
        return to;
    }

    public void setTo(Endpoint to) {
        this.to = to;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processKeepAlive(this);
    }

    public boolean isMarshallAware() {
        return false;
    }

    public boolean isMessageDispatchNotification() {
        return false;
    }

    public boolean isShutdownInfo() {
        return false;
    }

    public String toString() {
        return IntrospectionSupport.toString(this, KeepAliveInfo.class);
    }
}
