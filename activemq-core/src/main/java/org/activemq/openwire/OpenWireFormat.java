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
package org.activemq.openwire;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.activeio.ByteArrayOutputStream;
import org.activeio.ByteSequence;
import org.activeio.Packet;
import org.activeio.adapter.PacketToInputStream;
import org.activeio.command.ClassLoading;
import org.activeio.command.WireFormat;
import org.activeio.packet.ByteArrayPacket;
import org.activemq.command.CommandTypes;
import org.activemq.command.DataStructure;
import org.activemq.command.MarshallAware;

/**
 * 
 * @version $Revision$
 */
final public class OpenWireFormat implements WireFormat {
    
    static final byte NULL_TYPE = CommandTypes.NULL;
    private static final int MARSHAL_CACHE_SIZE = Short.MAX_VALUE/2;
    private DataStreamMarshaller dataMarshallers[];
    private int version;
    private boolean stackTraceEnabled=true;
    private boolean tcpNoDelayEnabled=false;
    private boolean cacheEnabled=true;

    private HashMap marshallCacheMap = new HashMap();
    private short nextMarshallCacheIndex=0;    
    private short lasMarshallCacheEvictionIndex=100;    
    private DataStructure marshallCache[] = new DataStructure[MARSHAL_CACHE_SIZE];
    private DataStructure unmarshallCache[] = new DataStructure[MARSHAL_CACHE_SIZE];
    
    public OpenWireFormat() {
        this(true);
    }
    
    public OpenWireFormat(boolean cacheEnabled) {
        setVersion(1);
        setCacheEnabled(cacheEnabled);
    }
    
    public int hashCode() {
        return  version 
            ^ (cacheEnabled?0x10000000:0x20000000)
            ^ (stackTraceEnabled?0x30000000:0x40000000);
    }
    
    public boolean equals(Object object) {
        if( object == null )
            return false;
        OpenWireFormat o = (OpenWireFormat) object;
        return o.stackTraceEnabled == stackTraceEnabled &&
            o.cacheEnabled == cacheEnabled &&
            o.version == version;
    }
    
    public String toString() {
        return "OpenWireFormat{version="+version+", cacheEnabled="+cacheEnabled+", stackTraceEnabled="+stackTraceEnabled+"}";
    }
    
    public int getVersion() {
        return version;
    }
    
    public Packet marshal(Object command) throws IOException {
        
        MarshallAware ma=null;
        // If not using value caching, then the marshaled form is always the same
        if( !cacheEnabled && ((DataStructure)command).isMarshallAware() ) {
            ma = (MarshallAware) command;
        }
        
        ByteSequence sequence=null;
        if( ma!=null ) {
            sequence = ma.getCachedMarshalledForm(this);
        }
        
        if( sequence == null ) {
            
            int size=1;
            if( command != null) {
                
                DataStructure c = (DataStructure) command;
                byte type = c.getDataStructureType();
                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
                if( dsm == null )
                    throw new IOException("Unknown data type: "+type);
                BooleanStream bs = new BooleanStream();
                size += dsm.marshal1(this, c, bs);
                size += bs.marshalledSize();

                ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
                DataOutputStream ds = new DataOutputStream(baos);
                ds.writeInt(size);
                ds.writeByte(type);
                bs.marshal(ds);
                dsm.marshal2(this, c, ds, bs);                
                ds.close();
                
                sequence = baos.toByteSequence();
                
            } else {
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream(5);
                DataOutputStream daos = new DataOutputStream(baos);
                daos.writeInt(size);
                daos.writeByte(NULL_TYPE);
                daos.close();
                sequence = baos.toByteSequence();
            }
            
            if( ma!=null ) {
                ma.setCachedMarshalledForm(this, sequence);
            }
        }
        return new ByteArrayPacket(sequence);
    }
    
    public Object unmarshal(Packet packet) throws IOException {
        ByteSequence sequence = packet.asByteSequence();
        DataInputStream dis = new DataInputStream(new PacketToInputStream(packet));
        int size = dis.readInt();
        if( sequence.getLength() != size+4 )
            System.out.println("Packet size does not match marshaled size: "+size+", "+(sequence.getLength()-4));
//            throw new IOException("Packet size does not match marshaled size");        
        Object command = doUnmarshal(dis);
        if( !cacheEnabled && ((DataStructure)command).isMarshallAware() ) {
            ((MarshallAware) command).setCachedMarshalledForm(this, sequence);
        }
        return command;
    }
    
    public void marshal(Object o, DataOutputStream ds) throws IOException {
        int size=1;
        if( o != null) {
            DataStructure c = (DataStructure) o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+type);

            BooleanStream bs = new BooleanStream();
            size += dsm.marshal1(this, c, bs);
            size += bs.marshalledSize(); 

            ds.writeInt(size);
            ds.writeByte(type);            
            bs.marshal(ds);
            dsm.marshal2(this, c, ds, bs);
        } else {
            ds.writeInt(size);
            ds.writeByte(NULL_TYPE);
        }
    }
    
    public Object unmarshal(DataInputStream dis) throws IOException {
        dis.readInt();
        return doUnmarshal(dis);
    }
    
    /**
     * Allows you to dynamically switch the version of the openwire protocol being used.
     * @param version
     */
    public void setVersion(int version) {
        String mfName = "org.activemq.openwire.v"+version+".MarshallerFactory";
        Class mfClass;
        try {
            mfClass = ClassLoading.loadClass(mfName, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw (IllegalArgumentException)new IllegalArgumentException("Invalid version: "+version+", could not load "+mfName).initCause(e);
        }
        try {
            Method method = mfClass.getMethod("createMarshallerMap", new Class[]{OpenWireFormat.class});
            dataMarshallers = (DataStreamMarshaller[]) method.invoke(null, new Object[]{this});
        } catch (Throwable e) {
            throw (IllegalArgumentException)new IllegalArgumentException("Invalid version: "+version+", "+mfName+" does not properly implement the createMarshallerMap method.").initCause(e);
        }
        this.version = version;
    }
        
    public Object doUnmarshal(DataInputStream dis) throws IOException {
        byte dataType = dis.readByte();
        if( dataType!=NULL_TYPE ) {
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+dataType);
            Object data = dsm.createObject();
            BooleanStream bs = new BooleanStream();
            bs.unmarshal(dis);
            dsm.unmarshal(this, data, dis, bs);
            return data;
        } else {
            return null;
        }
    }
    
    public int marshal1NestedObject(DataStructure o, BooleanStream bs) throws IOException {
        bs.writeBoolean(o != null);
        if( o == null ) 
            return 0;

        if( o.isMarshallAware() ) {
            MarshallAware ma = (MarshallAware) o;
            ByteSequence sequence=ma.getCachedMarshalledForm(this);
            bs.writeBoolean(sequence!=null);
            if( sequence!=null ) {
                return 1 + sequence.getLength();           
            }
        }
        
        byte type = o.getDataStructureType();
        DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
        if( dsm == null )
            throw new IOException("Unknown data type: "+type);
        return 1 + dsm.marshal1(this, o, bs);
    }
    
    public void marshal2NestedObject(DataStructure o, DataOutputStream ds, BooleanStream bs) throws IOException {
        if( !bs.readBoolean() ) 
            return;
            
        byte type = o.getDataStructureType();
        ds.writeByte(type);

        if( o.isMarshallAware() && bs.readBoolean() ) {
                        
            MarshallAware ma = (MarshallAware) o;
            ByteSequence sequence=ma.getCachedMarshalledForm(this);
            ds.write(sequence.getData(), sequence.getOffset(), sequence.getLength());
            
        } else {
            
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+type);
            dsm.marshal2(this, o, ds, bs);
            
        }
    }
    
    public DataStructure unmarshalNestedObject(DataInputStream dis, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            
            byte dataType = dis.readByte();
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+dataType);
            DataStructure data = dsm.createObject();

            if( data.isMarshallAware() && bs.readBoolean() ) {
                
                dis.readInt();
                dis.readByte();
                
                BooleanStream bs2 = new BooleanStream();
                bs2.unmarshal(dis);
                dsm.unmarshal(this, data, dis, bs2);

                // TODO: extract the sequence from the dis and associate it.
//                MarshallAware ma = (MarshallAware)data
//                ma.setCachedMarshalledForm(this, sequence);
                
            } else {
                dsm.unmarshal(this, data, dis, bs);
            }
            
            return data;
        } else {
            return null;
        }
    }
    
    public Short getMarshallCacheIndex(Object o) {
        return (Short) marshallCacheMap.get(o);
    }
    
    public Short addToMarshallCache(Object o) {
        Short index = new Short(nextMarshallCacheIndex++);
        if( nextMarshallCacheIndex >= MARSHAL_CACHE_SIZE ) {
            nextMarshallCacheIndex=0;
        }
        lasMarshallCacheEvictionIndex++;
        if( lasMarshallCacheEvictionIndex >= MARSHAL_CACHE_SIZE ) {
            lasMarshallCacheEvictionIndex=0;
        }
        if( marshallCache[lasMarshallCacheEvictionIndex]!=null ) {
            marshallCacheMap.remove(marshallCache[lasMarshallCacheEvictionIndex]);
            marshallCache[lasMarshallCacheEvictionIndex]=null;
        }
        marshallCacheMap.put(o, index);
        return index;
    }
    
    public void setInUnmarshallCache(short index, DataStructure o) {
        unmarshallCache[index]=o;
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

}
