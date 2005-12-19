/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
 *
 **/
package org.activemq.command;

import org.activemq.state.CommandVisitor;

import java.util.Arrays;

/**
 * 
 * @openwire:marshaller
 * @version $Revision$
 */
public class WireFormatInfo implements Command {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.WIREFORMAT_INFO;
    static final private byte MAGIC[] = new byte[] { 'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q' };

    static final public long STACK_TRACE_MASK = 0x00000001;
    static final public long TCP_NO_DELAY_MASK = 0x00000002;
    static final public long CACHE_MASK = 0x00000004;
    static final public long COMPRESSION_MASK = 0x00000008;

    protected int version;
    protected byte magic[] = MAGIC;
    protected int options;

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isWireFormatInfo() {
        return true;
    }

    /**
     * @openwire:property version=1 size=8
     */
    public byte[] getMagic() {
        return magic;
    }

    public void setMagic(byte[] magic) {
        this.magic = magic;
    }

    /**
     * @openwire:property version=1
     */
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isValid() {
        return magic != null && Arrays.equals(magic, MAGIC);
    }

    public void setCommandId(short value) {
    }

    public short getCommandId() {
        return 0;
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

    public void setResponseRequired(boolean responseRequired) {
    }

    public String toString() {
        return "WireFormatInfo {version=" + version + "}";
    }

    /**
     * @openwire:property version=1
     */
    public int getOptions() {
        return options;
    }

    public void setOptions(int options) {
        this.options = options;
    }

    public boolean isStackTraceEnabled() {
        return (options & STACK_TRACE_MASK) != 0;
    }

    public void setStackTraceEnabled(boolean enable) {
        if (enable) {
            options |= STACK_TRACE_MASK;
        }
        else {
            options &= ~STACK_TRACE_MASK;
        }
    }

    public boolean isTcpNoDelayEnabled() {
        return (options & TCP_NO_DELAY_MASK) != 0;
    }

    public void setTcpNoDelayEnabled(boolean enable) {
        if (enable) {
            options |= TCP_NO_DELAY_MASK;
        }
        else {
            options &= ~TCP_NO_DELAY_MASK;
        }
    }

    public boolean isCacheEnabled() {
        return (options & CACHE_MASK) != 0;
    }

    public void setCacheEnabled(boolean enable) {
        if (enable) {
            options |= CACHE_MASK;
        }
        else {
            options &= ~CACHE_MASK;
        }
    }

    public Response visit(CommandVisitor visitor) throws Throwable {
        return visitor.processWireFormat(this);
    }

    public boolean isMarshallAware() {
        return false;
    }

}
