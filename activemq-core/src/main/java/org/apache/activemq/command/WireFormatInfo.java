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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * @openwire:marshaller code="1"
 * @version $Revision$
 */
public class WireFormatInfo implements Command, MarshallAware {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.WIREFORMAT_INFO;
    private static final int MAX_PROPERTY_SIZE = 1024 * 4;
    private static final byte MAGIC[] = new byte[] {'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q'};

    protected byte magic[] = MAGIC;
    protected int version;
    protected ByteSequence marshalledProperties;

    protected transient Map<String, Object> properties;
    private transient Endpoint from;
    private transient Endpoint to;

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isWireFormatInfo() {
        return true;
    }

    public boolean isMarshallAware() {
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

    /**
     * @openwire:property version=1
     */
    public ByteSequence getMarshalledProperties() {
        return marshalledProperties;
    }

    public void setMarshalledProperties(ByteSequence marshalledProperties) {
        this.marshalledProperties = marshalledProperties;
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

    // ////////////////////
    // 
    // Implementation Methods.
    //
    // ////////////////////

    public Object getProperty(String name) throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                return null;
            }
            properties = unmarsallProperties(marshalledProperties);
        }
        return properties.get(name);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getProperties() throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                return Collections.EMPTY_MAP;
            }
            properties = unmarsallProperties(marshalledProperties);
        }
        return Collections.unmodifiableMap(properties);
    }

    public void clearProperties() {
        marshalledProperties = null;
        properties = null;
    }

    public void setProperty(String name, Object value) throws IOException {
        lazyCreateProperties();
        properties.put(name, value);
    }

    protected void lazyCreateProperties() throws IOException {
        if (properties == null) {
            if (marshalledProperties == null) {
                properties = new HashMap<String, Object>();
            } else {
                properties = unmarsallProperties(marshalledProperties);
                marshalledProperties = null;
            }
        }
    }

    private Map<String, Object> unmarsallProperties(ByteSequence marshalledProperties) throws IOException {
        return MarshallingSupport.unmarshalPrimitiveMap(new DataInputStream(new ByteArrayInputStream(marshalledProperties)), MAX_PROPERTY_SIZE);
    }

    public void beforeMarshall(WireFormat wireFormat) throws IOException {
        // Need to marshal the properties.
        if (marshalledProperties == null && properties != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(baos);
            MarshallingSupport.marshalPrimitiveMap(properties, os);
            os.close();
            marshalledProperties = baos.toByteSequence();
        }
    }

    public void afterMarshall(WireFormat wireFormat) throws IOException {
    }

    public void beforeUnmarshall(WireFormat wireFormat) throws IOException {
    }

    public void afterUnmarshall(WireFormat wireFormat) throws IOException {
    }

    public boolean isValid() {
        return magic != null && Arrays.equals(magic, MAGIC);
    }

    public void setResponseRequired(boolean responseRequired) {
    }

    /**
     * @throws IOException
     */
    public boolean isCacheEnabled() throws IOException {
        return Boolean.TRUE == getProperty("CacheEnabled");
    }

    public void setCacheEnabled(boolean cacheEnabled) throws IOException {
        setProperty("CacheEnabled", cacheEnabled ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * @throws IOException
     */
    public boolean isStackTraceEnabled() throws IOException {
        return Boolean.TRUE == getProperty("StackTraceEnabled");
    }

    public void setStackTraceEnabled(boolean stackTraceEnabled) throws IOException {
        setProperty("StackTraceEnabled", stackTraceEnabled ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * @throws IOException
     */
    public boolean isTcpNoDelayEnabled() throws IOException {
        return Boolean.TRUE == getProperty("TcpNoDelayEnabled");
    }

    public void setTcpNoDelayEnabled(boolean tcpNoDelayEnabled) throws IOException {
        setProperty("TcpNoDelayEnabled", tcpNoDelayEnabled ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * @throws IOException
     */
    public boolean isSizePrefixDisabled() throws IOException {
        return Boolean.TRUE == getProperty("SizePrefixDisabled");
    }

    public void setSizePrefixDisabled(boolean prefixPacketSize) throws IOException {
        setProperty("SizePrefixDisabled", prefixPacketSize ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * @throws IOException
     */
    public boolean isTightEncodingEnabled() throws IOException {
        return Boolean.TRUE == getProperty("TightEncodingEnabled");
    }

    public void setTightEncodingEnabled(boolean tightEncodingEnabled) throws IOException {
        setProperty("TightEncodingEnabled", tightEncodingEnabled ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * @throws IOException
     */
    public long getMaxInactivityDuration() throws IOException {
        Long l = (Long)getProperty("MaxInactivityDuration");
        return l == null ? 0 : l.longValue();
    }

    public void setMaxInactivityDuration(long maxInactivityDuration) throws IOException {
        setProperty("MaxInactivityDuration", new Long(maxInactivityDuration));
    }
    
    public long getMaxInactivityDurationInitalDelay() throws IOException {
        Long l = (Long)getProperty("MaxInactivityDurationInitalDelay");
        return l == null ? 0 : l.longValue();
    }

    public void setMaxInactivityDurationInitalDelay(long maxInactivityDurationInitalDelay) throws IOException {
        setProperty("MaxInactivityDurationInitalDelay", new Long(maxInactivityDurationInitalDelay));
    }
    
   

    /**
     * @throws IOException
     */
    public int getCacheSize() throws IOException {
        Integer i = (Integer)getProperty("CacheSize");
        return i == null ? 0 : i.intValue();
    }

    public void setCacheSize(int cacheSize) throws IOException {
        setProperty("CacheSize", new Integer(cacheSize));
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processWireFormat(this);
    }

    @Override
    public String toString() {
        Map<String, Object> p = null;
        try {
            p = getProperties();
        } catch (IOException ignore) {
        }
        return "WireFormatInfo { version=" + version + ", properties=" + p + ", magic=" + toString(magic) + "}";
    }

    private String toString(byte[] data) {
        StringBuffer sb = new StringBuffer();
        sb.append('[');
        for (int i = 0; i < data.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append((char)data[i]);
        }
        sb.append(']');
        return sb.toString();
    }

    // /////////////////////////////////////////////////////////////
    //
    // This are not implemented.
    //
    // /////////////////////////////////////////////////////////////

    public void setCommandId(int value) {
    }

    public int getCommandId() {
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

    public boolean isMessageDispatchNotification() {
        return false;
    }

    public boolean isShutdownInfo() {
        return false;
    }
    
    public boolean isConnectionControl() {
        return false;
    }

    public void setCachedMarshalledForm(WireFormat wireFormat, ByteSequence data) {
    }

    public ByteSequence getCachedMarshalledForm(WireFormat wireFormat) {
        return null;
    }

}
