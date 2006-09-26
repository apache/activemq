/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.openwire;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.MarshallAware;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.ClassLoading;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.wireformat.WireFormat;

/**
 * 
 * @version $Revision$
 */
final public class OpenWireFormat implements WireFormat {
    
    static final byte NULL_TYPE = CommandTypes.NULL;
    private static final int MARSHAL_CACHE_SIZE = Short.MAX_VALUE/2;
    private static final int MARSHAL_CACHE_PREFERED_SIZE = MARSHAL_CACHE_SIZE-100;
    
    private DataStreamMarshaller dataMarshallers[];
    private int version = 2;
    private boolean stackTraceEnabled=false;
    private boolean tcpNoDelayEnabled=false;
    private boolean cacheEnabled=false;
    private boolean tightEncodingEnabled=false;
    private boolean sizePrefixDisabled=false;

    private HashMap marshallCacheMap = new HashMap();
    private short nextMarshallCacheIndex=0;    
    private short nextMarshallCacheEvictionIndex=0;
    
    private DataStructure marshallCache[] = new DataStructure[MARSHAL_CACHE_SIZE];
    private DataStructure unmarshallCache[] = new DataStructure[MARSHAL_CACHE_SIZE];
	private WireFormatInfo preferedWireFormatInfo;
            
	public OpenWireFormat() {
		this(1);
	}
	
    public OpenWireFormat(int i) {
		setVersion(i);
	}

	public int hashCode() {
        return  version 
            ^ (cacheEnabled         ? 0x10000000:0x20000000)
            ^ (stackTraceEnabled    ? 0x01000000:0x02000000)
            ^ (tightEncodingEnabled ? 0x00100000:0x00200000)
            ^ (sizePrefixDisabled     ? 0x00010000:0x00020000)
            ;
    }
    
    public OpenWireFormat copy() {
        OpenWireFormat answer = new OpenWireFormat();
        answer.version = version;
        answer.stackTraceEnabled = stackTraceEnabled;
        answer.tcpNoDelayEnabled = tcpNoDelayEnabled;
        answer.cacheEnabled = cacheEnabled;
        answer.tightEncodingEnabled = tightEncodingEnabled;
        answer.sizePrefixDisabled = sizePrefixDisabled;
        answer.preferedWireFormatInfo = preferedWireFormatInfo;
        return answer;
    }
    
    public boolean equals(Object object) {
        if( object == null )
            return false;
        OpenWireFormat o = (OpenWireFormat) object;
        return o.stackTraceEnabled == stackTraceEnabled &&
            o.cacheEnabled == cacheEnabled &&
            o.version == version && 
            o.tightEncodingEnabled == tightEncodingEnabled && 
            o.sizePrefixDisabled == sizePrefixDisabled 
            ;
    }
    
    static IdGenerator g = new IdGenerator();
    String id = g.generateId();
    public String toString() {
        return "OpenWireFormat{version="+version+", cacheEnabled="+cacheEnabled+", stackTraceEnabled="+stackTraceEnabled+", tightEncodingEnabled="+tightEncodingEnabled+", sizePrefixDisabled="+sizePrefixDisabled+"}";
        //return "OpenWireFormat{id="+id+", tightEncodingEnabled="+tightEncodingEnabled+"}";
    }
    
    public int getVersion() {
        return version;
    }
    
    public ByteSequence marshal(Object command) throws IOException {
        
        if( cacheEnabled ) {
            runMarshallCacheEvictionSweep();
        }
        
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
                
                if( tightEncodingEnabled ) {
                    
                    BooleanStream bs = new BooleanStream();
                    size += dsm.tightMarshal1(this, c, bs);
                    size += bs.marshalledSize();
    
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
                    DataOutputStream ds = new DataOutputStream(baos);
                    if( !sizePrefixDisabled ) {
                        ds.writeInt(size);
                    }
                    ds.writeByte(type);
                    bs.marshal(ds);
                    dsm.tightMarshal2(this, c, ds, bs);                
                    ds.close();
                    sequence = baos.toByteSequence();
                    
                } else {
                    
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream ds = new DataOutputStream(baos);
                    if( !sizePrefixDisabled ) {
                        ds.writeInt(0); // we don't know the final size yet but write this here for now.
                    }
                    ds.writeByte(type);
                    dsm.looseMarshal(this, c, ds);                
                    ds.close();
                    sequence = baos.toByteSequence();
                    
                    if( !sizePrefixDisabled ) {
                        size = sequence.getLength()-4;
                        int pos = sequence.offset;
                        ByteSequenceData.writeIntBig(sequence, size);
                        sequence.offset = pos;
                    }
                }
                
                
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
        return sequence;
    }
    
    public Object unmarshal(ByteSequence sequence) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(sequence));
        
        if( !sizePrefixDisabled ) {
            int size = dis.readInt();
            if( sequence.getLength()-4 != size ) {
    //            throw new IOException("Packet size does not match marshaled size");
            }
        }
        
        Object command = doUnmarshal(dis);
        if( !cacheEnabled && ((DataStructure)command).isMarshallAware() ) {
            ((MarshallAware) command).setCachedMarshalledForm(this, sequence);
        }
        return command;
    }
    
    public void marshal(Object o, DataOutputStream dataOut) throws IOException {
        
        if( cacheEnabled ) {
            runMarshallCacheEvictionSweep();
        }
        
        int size=1;
        if( o != null) {
        	
            DataStructure c = (DataStructure) o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+type);

            if( tightEncodingEnabled ) {
	            BooleanStream bs = new BooleanStream();
	            size += dsm.tightMarshal1(this, c, bs);
	            size += bs.marshalledSize(); 

                if( !sizePrefixDisabled ) {
                    dataOut.writeInt(size);
                }
                
	            dataOut.writeByte(type);            
	            bs.marshal(dataOut);
	            dsm.tightMarshal2(this, c, dataOut, bs);
                
            } else {            	
            	DataOutputStream looseOut = dataOut;
            	ByteArrayOutputStream baos=null;
            	
            	if( !sizePrefixDisabled ) {
	                baos = new ByteArrayOutputStream();
	                looseOut = new DataOutputStream(baos);
                }
                
                looseOut.writeByte(type);
                dsm.looseMarshal(this, c, looseOut);
                
                if( !sizePrefixDisabled ) {
                    looseOut.close();
                    ByteSequence sequence = baos.toByteSequence();
                    dataOut.writeInt(sequence.getLength());
                    dataOut.write(sequence.getData(), sequence.getOffset(), sequence.getLength());
                }

            }
            
        } else {
            dataOut.writeInt(size);
            dataOut.writeByte(NULL_TYPE);
        }
    }

    public Object unmarshal(DataInputStream dis) throws IOException {
        if( !sizePrefixDisabled ) {
        	dis.readInt();
        }
        return doUnmarshal(dis);
    }
    
    /**
     * Used by NIO or AIO transports
     */
    public int tightMarshal1(Object o, BooleanStream bs) throws IOException {
        int size=1;
        if( o != null) {
            DataStructure c = (DataStructure) o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+type);

            size += dsm.tightMarshal1(this, c, bs);
            size += bs.marshalledSize(); 
        }
        return size;
    }
    
    /**
     * Used by NIO or AIO transports; note that the size is not written as part of this method.
     */
    public void tightMarshal2(Object o, DataOutputStream ds, BooleanStream bs) throws IOException {
        if( cacheEnabled ) {
            runMarshallCacheEvictionSweep();
        }
        
        if( o != null) {
            DataStructure c = (DataStructure) o;
            byte type = c.getDataStructureType();
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+type);

            ds.writeByte(type);            
            bs.marshal(ds);
            dsm.tightMarshal2(this, c, ds, bs);            
        } 
    }

    
    /**
     * Allows you to dynamically switch the version of the openwire protocol being used.
     * @param version
     */
    public void setVersion(int version) {
        String mfName = "org.apache.activemq.openwire.v"+version+".MarshallerFactory";
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
            if( this.tightEncodingEnabled ) {
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

//    public void debug(String msg) {
//    	String t = (Thread.currentThread().getName()+"                                         ").substring(0, 40);
//    	System.out.println(t+": "+msg);
//    }
    public int tightMarshalNestedObject1(DataStructure o, BooleanStream bs) throws IOException {
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
        return 1 + dsm.tightMarshal1(this, o, bs);
    }
    
    public void tightMarshalNestedObject2(DataStructure o, DataOutputStream ds, BooleanStream bs) throws IOException {
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
            dsm.tightMarshal2(this, o, ds, bs);
            
        }
    }
    
    public DataStructure tightUnmarshalNestedObject(DataInputStream dis, BooleanStream bs) throws IOException {
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
                dsm.tightUnmarshal(this, data, dis, bs2);

                // TODO: extract the sequence from the dis and associate it.
//                MarshallAware ma = (MarshallAware)data
//                ma.setCachedMarshalledForm(this, sequence);
                
            } else {
                dsm.tightUnmarshal(this, data, dis, bs);
            }
            
            return data;
        } else {
            return null;
        }
    }
    
    public DataStructure looseUnmarshalNestedObject(DataInputStream dis) throws IOException {
        if( dis.readBoolean() ) {
            
            byte dataType = dis.readByte();
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+dataType);
            DataStructure data = dsm.createObject();
            dsm.looseUnmarshal(this, data, dis);
            return data;
            
        } else {
            return null;
        }
    }

    public void looseMarshalNestedObject(DataStructure o, DataOutputStream dataOut) throws IOException {
        dataOut.writeBoolean(o!=null);
        if( o!=null ) {
            byte type = o.getDataStructureType();
            dataOut.writeByte(type);
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if( dsm == null )
                throw new IOException("Unknown data type: "+type);
            dsm.looseMarshal(this, o, dataOut);
        }
    }

    public void runMarshallCacheEvictionSweep() {
        // Do we need to start evicting??
        while( marshallCacheMap.size() > MARSHAL_CACHE_PREFERED_SIZE ) {
            
            marshallCacheMap.remove(marshallCache[nextMarshallCacheEvictionIndex]);
            marshallCache[nextMarshallCacheEvictionIndex]=null;

            nextMarshallCacheEvictionIndex++;
            if( nextMarshallCacheEvictionIndex >= MARSHAL_CACHE_SIZE ) {
                nextMarshallCacheEvictionIndex=0;
            }
            
        }
    }
    
    public Short getMarshallCacheIndex(DataStructure o) {
        return (Short) marshallCacheMap.get(o);
    }
    
    public Short addToMarshallCache(DataStructure o) {
        short i = nextMarshallCacheIndex++;
        if( nextMarshallCacheIndex >= MARSHAL_CACHE_SIZE ) {
            nextMarshallCacheIndex=0;
        }
        
        // We can only cache that item if there is space left.
        if( marshallCacheMap.size() < MARSHAL_CACHE_SIZE ) {
            marshallCache[i] = o;
            Short index = new Short(i);
            marshallCacheMap.put(o, index);
            return index;
        } else {
            // Use -1 to indicate that the value was not cached due to cache being full.
            return new Short((short)-1);
        }
    }
    
    public void setInUnmarshallCache(short index, DataStructure o) {
        
        // There was no space left in the cache, so we can't
        // put this in the cache.
        if( index == -1 )
            return;
        
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

	public void renegotiateWireFormat(WireFormatInfo info) throws IOException {
		
		if( preferedWireFormatInfo==null )
			throw new IllegalStateException("Wireformat cannot not be renegotiated.");
		
		this.setVersion(min(preferedWireFormatInfo.getVersion(), info.getVersion()) );
		this.stackTraceEnabled = info.isStackTraceEnabled() && preferedWireFormatInfo.isStackTraceEnabled();
		this.tcpNoDelayEnabled = info.isTcpNoDelayEnabled() && preferedWireFormatInfo.isTcpNoDelayEnabled();
		this.cacheEnabled = info.isCacheEnabled() && preferedWireFormatInfo.isCacheEnabled();
		this.tightEncodingEnabled = info.isTightEncodingEnabled() && preferedWireFormatInfo.isTightEncodingEnabled();
		this.sizePrefixDisabled = info.isSizePrefixDisabled() && preferedWireFormatInfo.isSizePrefixDisabled();
		
	}

    protected int min(int version1, int version2) {
        if (version1 < version2 && version1 > 0 || version2 <= 0) {
            return version1;
        }
        return version2;
    }
}
