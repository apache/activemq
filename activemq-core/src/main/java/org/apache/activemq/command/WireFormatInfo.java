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
import org.apache.activemq.util.IntrospectionSupport;

import java.util.Arrays;

/**
 * 
 * @openwire:marshaller code="1"
 * @version $Revision$
 */
public class WireFormatInfo implements Command {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.WIREFORMAT_INFO;
    static final private byte MAGIC[] = new byte[] { 'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q' };

    protected int version;
    protected byte magic[] = MAGIC;

    protected boolean stackTraceEnabled;
    protected boolean tcpNoDelayEnabled;
    protected boolean cacheEnabled;
    protected boolean tightEncodingEnabled;
    protected boolean prefixPacketSize;

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isWireFormatInfo() {
        return true;
    }

    /**
     * @openwire:property version=1 size=8 testSize=-1
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

    /**
     * @openwire:property version=1
     */
    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isStackTraceEnabled() {
        return stackTraceEnabled;
    }

    public void setStackTraceEnabled(boolean stackTraceEnabled) {
        this.stackTraceEnabled = stackTraceEnabled;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isTcpNoDelayEnabled() {
        return tcpNoDelayEnabled;
    }

    public void setTcpNoDelayEnabled(boolean tcpNoDelayEnabled) {
        this.tcpNoDelayEnabled = tcpNoDelayEnabled;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isPrefixPacketSize() {
        return prefixPacketSize;
    }
    public void setPrefixPacketSize(boolean prefixPacketSize) {
        this.prefixPacketSize = prefixPacketSize;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isTightEncodingEnabled() {
        return tightEncodingEnabled;
    }
    public void setTightEncodingEnabled(boolean tightEncodingEnabled) {
        this.tightEncodingEnabled = tightEncodingEnabled;
    }

    public Response visit(CommandVisitor visitor) throws Throwable {
        return visitor.processWireFormat(this);
    }

    public boolean isMarshallAware() {
        return false;
    }

    public boolean isMessageDispatchNotification(){
        return false;
    }
    
    public boolean isShutdownInfo(){
        return false;
    }

    public String toString() {
        return IntrospectionSupport.toString(this, WireFormatInfo.class);
    }
}
