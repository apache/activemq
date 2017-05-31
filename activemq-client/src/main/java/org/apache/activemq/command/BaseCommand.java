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

import java.util.Map;

import org.apache.activemq.util.IntrospectionSupport;

/**
 *
 * @openwire:marshaller
 *
 */
public abstract class BaseCommand implements Command {

    protected int commandId;
    protected boolean responseRequired;

    private transient Endpoint from;
    private transient Endpoint to;

    public void copy(BaseCommand copy) {
        copy.commandId = commandId;
        copy.responseRequired = responseRequired;
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
     * @openwire:property version=1
     */
    @Override
    public boolean isResponseRequired() {
        return responseRequired;
    }

    @Override
    public void setResponseRequired(boolean responseRequired) {
        this.responseRequired = responseRequired;
    }

    @Override
    public String toString() {
        return toString(null);
    }

    public String toString(Map<String, Object> overrideFields) {
        return IntrospectionSupport.toString(this, BaseCommand.class, overrideFields);
    }

    @Override
    public boolean isWireFormatInfo() {
        return false;
    }

    @Override
    public boolean isBrokerInfo() {
        return false;
    }

    @Override
    public boolean isResponse() {
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
    public boolean isMarshallAware() {
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

    /**
     * The endpoint within the transport where this message came from.
     */
    @Override
    public Endpoint getFrom() {
        return from;
    }

    @Override
    public void setFrom(Endpoint from) {
        this.from = from;
    }

    /**
     * The endpoint within the transport where this message is going to - null
     * means all endpoints.
     */
    @Override
    public Endpoint getTo() {
        return to;
    }

    @Override
    public void setTo(Endpoint to) {
        this.to = to;
    }
}
