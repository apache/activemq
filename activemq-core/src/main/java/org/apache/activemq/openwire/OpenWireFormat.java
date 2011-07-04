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
package org.apache.activemq.openwire;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.wireformat.WireFormat;

/**
 * 
 * 
 */
public final class OpenWireFormat implements WireFormat {

    public static final int DEFAULT_VERSION = CommandTypes.PROTOCOL_STORE_VERSION;
    public static final int DEFAULT_WIRE_VERSION = CommandTypes.PROTOCOL_VERSION;
    public static final int DEFAULT_MAX_FRAME_SIZE = 100 * 1024 * 1024; //100 MB

    static final byte NULL_TYPE = CommandTypes.NULL;
    private static final int MARSHAL_CACHE_SIZE = Short.MAX_VALUE / 2;
    private static final int MARSHAL_CACHE_FREE_SPACE = 100;

    private DataStreamMarshaller dataMarshallers[];
    private int version;
    private boolean stackTraceEnabled;
    private boolean tcpNoDelayEnabled;
    private boolean cacheEnabled;
    private boolean tightEncodingEnabled;
    private boolean sizePrefixDisabled;
    private long maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

    // The following fields are used for value caching
    private short nextMarshallCacheIndex;
    private short nextMarshallCacheEvictionIndex;
    private Map<DataStructure, Short> marshallCacheMap = new HashMap<DataStructure, Short>();
    private DataStructure marshallCache[] = new DataStructure[MARSHAL_CACHE_SIZE];
    private DataStructure unmarshallCache[] = new DataStructure[MARSHAL_CACHE_SIZE];
    private DataByteArrayOutputStream bytesOut = new DataByteArrayOutputStream();
    private DataByteArrayInputStream bytesIn = new DataByteArrayInputStream();
    private WireFormatInfo preferedWireFormatInfo;
    
    public OpenWireFormat() {
        this(DEFAULT_VERSION);
    }

    public OpenWireFormat(int i) {
        setVersion(i);
    }

    public int hashCode() {
        return version ^ (cacheEnabled ? 0x10000000 : 0x20000000)
               ^ (stackTraceEnabled ? 0x01000000 : 0x02000000)
               ^ (tightEncodingEnabled ? 0x00100000 : 0x00200000)
               ^ (sizePrefixDisabled ? 0x00010000 : 0x00020000);
    }

    public OpenWireFormat copy() {
        OpenWireFormat answer = new OpenWireFormat(version);
        answer.stackTraceEnabled = stackTraceEnabled;
        answer.tcpNoDelayEnabled = tcpNoDelayEnabled;
        answer.cacheEnabled = cacheEnabled;
        answer.tightEncodingEnabled = tightEncodingEnabled;
        answer.sizePrefixDisabled = sizePrefixDisabled;
        answer.preferedWireFormatInfo = preferedWireFormatInfo;
        return answer;
    }

    public boolean equals(Object object) {
        if (object == null) {
            return false;
        }
        OpenWireFormat o = (OpenWireFormat)object;
        return o.stackTraceEnabled == stackTraceEnabled && o.cacheEnabled == cacheEnabled
               && o.version == version && o.tightEncodingEnabled == tightEncodingEnabled
               && o.sizePrefixDisabled == sizePrefixDisabled;
    }


    public String toString() {
        return "OpenWireFormat{version=" + version + ", cacheEnabled=" + cacheEnabled + ", stackTraceEnabled=" + stackTraceEnabled + ", tightEncodingEnabled="
               + tightEncodingEnabled + ", sizePrefixDisabled=" + sizePrefixDisabled +  ", maxFrameSize=" + maxFrameSize + "}";
        // return "OpenWireFormat{id="+id+",
        // tightEncodingEnabled="+tightEncodingEnabled+"}";
    }

    public int getVersion() {
        return version;
    }

    public synchronized ByteSequence marshal(Object command) throws IOException {

        if (cacheEnabled) {
            runMarshallCacheEvictionSweep();
        }

//        MarshallAware ma = null;
//        // If not using value caching, then the marshaled form is always the
//        // same
//        if (!cacheEnabled && ((DataStructure)command).isMarshallAware()) {
//            ma = (MarshallAware)command;
//        }

        ByteSequence sequence = null;
        // if( ma!=null ) {
        // sequence = ma.getCachedMarshalledForm(this);
        // }

        if (sequence == null) {

            int size = 1;
            if (command != null) {

                DataStructure c = (DataStructure)command;
                byte type = c.getDataStructureType();
                DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
                if (dsm == null) {
                    throw new IOException("Unknown data type: " + type);
                }
                if (tightEncodingEnabled) {

                    BooleanStream bs = new BooleanStream();
                    size += dsm.tightMarshal1(this, c, bs);
                    size += bs.marshalledSize();

                    bytesOut.restart(size);
                    if (!sizePrefixDisabled) {
                        bytesOut.writeInt(size);
                    }
                    bytesOut.writeByte(type);
                    bs.marshal(bytesOut);
                    dsm.tightMarshal2(this, c, bytesOut, bs);
                    sequence = bytesOut.toByteSequence();

                } else {
                    bytesOut.restart();
                    if (!sizePrefixDisabled) {
                        bytesOut.writeInt(0); // we don't know the final size
                                                // yet but write this here for
                                                // now.
                    }
                    bytesOut.writeByte(type);
                    dsm.looseMarshal(this, c, bytesOut);
                    sequence = bytesOut.toByteSequence();

                    if (!sizePrefixDisabled) {
                        size = sequence.getLength() - 4;
                        int pos = sequence.offset;
                        ByteSequenceData.writeIntBig(sequence, size);
                        sequence.offset = pos;
                    }
                }

            } else {
                bytesOut.restart(5);
                bytesOut.writeInt(size);
                bytesOut.writeByte(NULL_TYPE);
                sequence = bytesOut.toByteSequence();
            }

            // if( ma!=null ) {
            // ma.setCachedMarshalledForm(this, sequence);
            // }
        }
        return sequence;
    }

    public synchronized Object unmarshal(ByteSequence sequence) throws IOException {
        bytesIn.restart(sequence);
        // DataInputStream dis = new DataInputStream(new
        // ByteArrayInputStream(sequence));

        if (!sizePrefixDisabled) {
            int size = bytesIn.readInt();
            if (sequence.getLength() - 4 != size) {
                // throw new IOException("Packet size does not match marshaled
                // size");
            }

            if (size > maxFrameSize) {
                throw new IOException("Frame size of " + (size / (1024 * 1024)) + " MB larger than max allowed " + (maxFrameSize / (1024 * 1024)) + " MB");
            }
        }

        Object command = doUnmarshal(bytesIn);
        // if( !cacheEnabled && ((DataStructure)command).isMarshallAware() ) {
        // ((MarshallAware) command).setCachedMarshalledForm(this, sequence);
        // }
        return command;
    }

    public synchronized void marshal(Object o, DataOutput dataOut) throws IOException {

        if (cacheEnabled) {
            runMarshallCacheEvictionSweep();
        }

        int size = 1;
        if (o != null) {

            DataStructure c = (DataStructure)o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + type);
            }
            if (tightEncodingEnabled) {
                BooleanStream bs = new BooleanStream();
                size += dsm.tightMarshal1(this, c, bs);
                size += bs.marshalledSize();

                if (!sizePrefixDisabled) {
                    dataOut.writeInt(size);
                }

                dataOut.writeByte(type);
                bs.marshal(dataOut);
                dsm.tightMarshal2(this, c, dataOut, bs);

            } else {
                DataOutput looseOut = dataOut;

                if (!sizePrefixDisabled) {
                    bytesOut.restart();
                    looseOut = bytesOut;
                }

                looseOut.writeByte(type);
                dsm.looseMarshal(this, c, looseOut);

                if (!sizePrefixDisabled) {
                    ByteSequence sequence = bytesOut.toByteSequence();
                    dataOut.writeInt(sequence.getLength());
                    dataOut.write(sequence.getData(), sequence.getOffset(), sequence.getLength());
                }

            }

        } else {
            if (!sizePrefixDisabled) {
            	dataOut.writeInt(size);
            }
            dataOut.writeByte(NULL_TYPE);
        }
    }

    public Object unmarshal(DataInput dis) throws IOException {
        DataInput dataIn = dis;
        if (!sizePrefixDisabled) {
            int size = dis.readInt();
            if (size > maxFrameSize) {
                throw new IOException("Frame size of " + (size / (1024 * 1024)) + " MB larger than max allowed " + (maxFrameSize / (1024 * 1024)) + " MB");
            }
            // int size = dis.readInt();
            // byte[] data = new byte[size];
            // dis.readFully(data);
            // bytesIn.restart(data);
            // dataIn = bytesIn;
        }
        return doUnmarshal(dataIn);
    }

    /**
     * Used by NIO or AIO transports
     */
    public int tightMarshal1(Object o, BooleanStream bs) throws IOException {
        int size = 1;
        if (o != null) {
            DataStructure c = (DataStructure)o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + type);
            }

            size += dsm.tightMarshal1(this, c, bs);
            size += bs.marshalledSize();
        }
        return size;
    }

    /**
     * Used by NIO or AIO transports; note that the size is not written as part
     * of this method.
     */
    public void tightMarshal2(Object o, DataOutput ds, BooleanStream bs) throws IOException {
        if (cacheEnabled) {
            runMarshallCacheEvictionSweep();
        }

        if (o != null) {
            DataStructure c = (DataStructure)o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + type);
            }
            ds.writeByte(type);
            bs.marshal(ds);
            dsm.tightMarshal2(this, c, ds, bs);
        }
    }

    /**
     * Allows you to dynamically switch the version of the openwire protocol
     * being used.
     * 
     * @param version
     */
    public void setVersion(int version) {
        String mfName = "org.apache.activemq.openwire.v" + version + ".MarshallerFactory";
        Class mfClass;
        try {
            mfClass = Class.forName(mfName, false, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw (IllegalArgumentException)new IllegalArgumentException("Invalid version: " + version
                                                                         + ", could not load " + mfName)
                .initCause(e);
        }
        try {
            Method method = mfClass.getMethod("createMarshallerMap", new Class[] {OpenWireFormat.class});
            dataMarshallers = (DataStreamMarshaller[])method.invoke(null, new Object[] {this});
        } catch (Throwable e) {
            throw (IllegalArgumentException)new IllegalArgumentException(
                                                                         "Invalid version: "
                                                                             + version
                                                                             + ", "
                                                                             + mfName
                                                                             + " does not properly implement the createMarshallerMap method.")
                .initCause(e);
        }
        this.version = version;
    }

    public Object doUnmarshal(DataInput dis) throws IOException {
        byte dataType = dis.readByte();
        if (dataType != NULL_TYPE) {
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[dataType & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + dataType);
            }
            Object data = dsm.createObject();
            if (this.tightEncodingEnabled) {
                BooleanStream bs = new BooleanStream();
                bs.unmarshal(dis);
                dsm.tightUnmarshal(this, data, dis, bs);
            } else {
                dsm.looseUnmarshal(this, data, dis);
            }
            return data;
        } else {
            return null;
        }
    }

    // public void debug(String msg) {
    // String t = (Thread.currentThread().getName()+" ").substring(0, 40);
    // System.out.println(t+": "+msg);
    // }
    public int tightMarshalNestedObject1(DataStructure o, BooleanStream bs) throws IOException {
        bs.writeBoolean(o != null);
        if (o == null) {
            return 0;
        }

        if (o.isMarshallAware()) {
            // MarshallAware ma = (MarshallAware)o;
            ByteSequence sequence = null;
            // sequence=ma.getCachedMarshalledForm(this);
            bs.writeBoolean(sequence != null);
            if (sequence != null) {
                return 1 + sequence.getLength();
            }
        }

        byte type = o.getDataStructureType();
        DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
        if (dsm == null) {
            throw new IOException("Unknown data type: " + type);
        }
        return 1 + dsm.tightMarshal1(this, o, bs);
    }

    public void tightMarshalNestedObject2(DataStructure o, DataOutput ds, BooleanStream bs)
        throws IOException {
        if (!bs.readBoolean()) {
            return;
        }

        byte type = o.getDataStructureType();
        ds.writeByte(type);

        if (o.isMarshallAware() && bs.readBoolean()) {

            // We should not be doing any caching
            throw new IOException("Corrupted stream");
            // MarshallAware ma = (MarshallAware) o;
            // ByteSequence sequence=ma.getCachedMarshalledForm(this);
            // ds.write(sequence.getData(), sequence.getOffset(),
            // sequence.getLength());

        } else {

            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + type);
            }
            dsm.tightMarshal2(this, o, ds, bs);

        }
    }

    public DataStructure tightUnmarshalNestedObject(DataInput dis, BooleanStream bs) throws IOException {
        if (bs.readBoolean()) {

            byte dataType = dis.readByte();
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[dataType & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + dataType);
            }
            DataStructure data = dsm.createObject();

            if (data.isMarshallAware() && bs.readBoolean()) {

                dis.readInt();
                dis.readByte();

                BooleanStream bs2 = new BooleanStream();
                bs2.unmarshal(dis);
                dsm.tightUnmarshal(this, data, dis, bs2);

                // TODO: extract the sequence from the dis and associate it.
                // MarshallAware ma = (MarshallAware)data
                // ma.setCachedMarshalledForm(this, sequence);

            } else {
                dsm.tightUnmarshal(this, data, dis, bs);
            }

            return data;
        } else {
            return null;
        }
    }

    public DataStructure looseUnmarshalNestedObject(DataInput dis) throws IOException {
        if (dis.readBoolean()) {

            byte dataType = dis.readByte();
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[dataType & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + dataType);
            }
            DataStructure data = dsm.createObject();
            dsm.looseUnmarshal(this, data, dis);
            return data;

        } else {
            return null;
        }
    }

    public void looseMarshalNestedObject(DataStructure o, DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(o != null);
        if (o != null) {
            byte type = o.getDataStructureType();
            dataOut.writeByte(type);
            DataStreamMarshaller dsm = (DataStreamMarshaller)dataMarshallers[type & 0xFF];
            if (dsm == null) {
                throw new IOException("Unknown data type: " + type);
            }
            dsm.looseMarshal(this, o, dataOut);
        }
    }

    public void runMarshallCacheEvictionSweep() {
        // Do we need to start evicting??
        while (marshallCacheMap.size() > marshallCache.length - MARSHAL_CACHE_FREE_SPACE) {

            marshallCacheMap.remove(marshallCache[nextMarshallCacheEvictionIndex]);
            marshallCache[nextMarshallCacheEvictionIndex] = null;

            nextMarshallCacheEvictionIndex++;
            if (nextMarshallCacheEvictionIndex >= marshallCache.length) {
                nextMarshallCacheEvictionIndex = 0;
            }

        }
    }

    public Short getMarshallCacheIndex(DataStructure o) {
        return marshallCacheMap.get(o);
    }

    public Short addToMarshallCache(DataStructure o) {
        short i = nextMarshallCacheIndex++;
        if (nextMarshallCacheIndex >= marshallCache.length) {
            nextMarshallCacheIndex = 0;
        }

        // We can only cache that item if there is space left.
        if (marshallCacheMap.size() < marshallCache.length) {
            marshallCache[i] = o;
            Short index = new Short(i);
            marshallCacheMap.put(o, index);
            return index;
        } else {
            // Use -1 to indicate that the value was not cached due to cache
            // being full.
            return new Short((short)-1);
        }
    }

    public void setInUnmarshallCache(short index, DataStructure o) {

        // There was no space left in the cache, so we can't
        // put this in the cache.
        if (index == -1) {
            return;
        }

        unmarshallCache[index] = o;
    }

    public DataStructure getFromUnmarshallCache(short index) {
        return unmarshallCache[index];
    }

    public void setStackTraceEnabled(boolean b) {
        stackTraceEnabled = b;
    }

    public boolean isStackTraceEnabled() {
        return stackTraceEnabled;
    }

    public boolean isTcpNoDelayEnabled() {
        return tcpNoDelayEnabled;
    }

    public void setTcpNoDelayEnabled(boolean tcpNoDelayEnabled) {
        this.tcpNoDelayEnabled = tcpNoDelayEnabled;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public boolean isTightEncodingEnabled() {
        return tightEncodingEnabled;
    }

    public void setTightEncodingEnabled(boolean tightEncodingEnabled) {
        this.tightEncodingEnabled = tightEncodingEnabled;
    }

    public boolean isSizePrefixDisabled() {
        return sizePrefixDisabled;
    }

    public void setSizePrefixDisabled(boolean prefixPacketSize) {
        this.sizePrefixDisabled = prefixPacketSize;
    }

    public void setPreferedWireFormatInfo(WireFormatInfo info) {
        this.preferedWireFormatInfo = info;
    }

    public WireFormatInfo getPreferedWireFormatInfo() {
        return preferedWireFormatInfo;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public void renegotiateWireFormat(WireFormatInfo info) throws IOException {

        if (preferedWireFormatInfo == null) {
            throw new IllegalStateException("Wireformat cannot not be renegotiated.");
        }

        this.setVersion(min(preferedWireFormatInfo.getVersion(), info.getVersion()));
        info.setVersion(this.getVersion());

        this.setMaxFrameSize(min(preferedWireFormatInfo.getMaxFrameSize(), info.getMaxFrameSize()));
        info.setMaxFrameSize(this.getMaxFrameSize());

        this.stackTraceEnabled = info.isStackTraceEnabled() && preferedWireFormatInfo.isStackTraceEnabled();
        info.setStackTraceEnabled(this.stackTraceEnabled);

        this.tcpNoDelayEnabled = info.isTcpNoDelayEnabled() && preferedWireFormatInfo.isTcpNoDelayEnabled();
        info.setTcpNoDelayEnabled(this.tcpNoDelayEnabled);

        this.cacheEnabled = info.isCacheEnabled() && preferedWireFormatInfo.isCacheEnabled();
        info.setCacheEnabled(this.cacheEnabled);

        this.tightEncodingEnabled = info.isTightEncodingEnabled()
                                    && preferedWireFormatInfo.isTightEncodingEnabled();
        info.setTightEncodingEnabled(this.tightEncodingEnabled);

        this.sizePrefixDisabled = info.isSizePrefixDisabled()
                                  && preferedWireFormatInfo.isSizePrefixDisabled();
        info.setSizePrefixDisabled(this.sizePrefixDisabled);

        if (cacheEnabled) {

            int size = Math.min(preferedWireFormatInfo.getCacheSize(), info.getCacheSize());
            info.setCacheSize(size);

            if (size == 0) {
                size = MARSHAL_CACHE_SIZE;
            }

            marshallCache = new DataStructure[size];
            unmarshallCache = new DataStructure[size];
            nextMarshallCacheIndex = 0;
            nextMarshallCacheEvictionIndex = 0;
            marshallCacheMap = new HashMap<DataStructure, Short>();
        } else {
            marshallCache = null;
            unmarshallCache = null;
            nextMarshallCacheIndex = 0;
            nextMarshallCacheEvictionIndex = 0;
            marshallCacheMap = null;
        }

    }

    protected int min(int version1, int version2) {
        if (version1 < version2 && version1 > 0 || version2 <= 0) {
            return version1;
        }
        return version2;
    }

    protected long min(long version1, long version2) {
        if (version1 < version2 && version1 > 0 || version2 <= 0) {
            return version1;
        }
        return version2;
    }
}
