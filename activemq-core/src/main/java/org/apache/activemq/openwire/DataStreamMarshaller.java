/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.openwire;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.activeio.command.ClassLoading;
import org.apache.activemq.command.DataStructure;

abstract public class DataStreamMarshaller {

    static final public Constructor STACK_TRACE_ELEMENT_CONSTRUCTOR;
    
    static {
        Constructor constructor=null;
        try {
            constructor = StackTraceElement.class.getConstructor(new Class[]{String.class, String.class, String.class, int.class});            
        } catch (Throwable e) {            
        }
        STACK_TRACE_ELEMENT_CONSTRUCTOR = constructor;
    }
    

    abstract public byte getDataStructureType();
    abstract public DataStructure createObject();
    
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
        return 0;
    }
    public void marshal2(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut, BooleanStream bs) throws IOException {        
    }
    public void unmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {        
    }
    
    public int marshal1Long(OpenWireFormat wireFormat, long o, BooleanStream bs) throws IOException {
        if( o == 0 ) {
            bs.writeBoolean(false);
            bs.writeBoolean(false);
            return 0;
        } else if ( (o & 0xFFFFFFFFFFFF0000l ) == 0 ) {
            bs.writeBoolean(false);
            bs.writeBoolean(true);            
            return 2;
        } else if ( (o & 0xFFFFFFFF00000000l ) == 0) {
            bs.writeBoolean(true);
            bs.writeBoolean(false);
            return 4;
        } else {
            bs.writeBoolean(true);
            bs.writeBoolean(true);
            return 8;
        }
    }
    public void marshal2Long(OpenWireFormat wireFormat, long o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            if( bs.readBoolean() ) {
                dataOut.writeLong(o);
            } else {
                dataOut.writeInt((int) o);
            }
        } else {
            if( bs.readBoolean() ) {
                dataOut.writeShort((int) o);
            }
        }
    }
    public long unmarshalLong(OpenWireFormat wireFormat, DataInputStream dataIn, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            if( bs.readBoolean() ) {
                return dataIn.readLong();
            } else {
                return dataIn.readInt();
            }
        } else {
            if( bs.readBoolean() ) {
                return dataIn.readShort();
            } else {
                return 0;
            }
        }
    }
    
    protected DataStructure unmarsalNestedObject(OpenWireFormat wireFormat, DataInputStream dataIn, BooleanStream bs) throws IOException {
        return wireFormat.unmarshalNestedObject(dataIn, bs);
    }    
    protected int marshal1NestedObject(OpenWireFormat wireFormat, DataStructure o, BooleanStream bs) throws IOException {
        return wireFormat.marshal1NestedObject(o, bs);
    }
    
    protected void marshal2NestedObject(OpenWireFormat wireFormat, DataStructure o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        wireFormat.marshal2NestedObject(o, dataOut, bs);
    }

    protected DataStructure unmarsalCachedObject(OpenWireFormat wireFormat, DataInputStream dataIn, BooleanStream bs) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            if( bs.readBoolean() ) {
                short index = dataIn.readShort();
                DataStructure object = wireFormat.unmarshalNestedObject(dataIn, bs);
                wireFormat.setInUnmarshallCache(index, object);
                return object;
            } else {
                short index = dataIn.readShort();
                return wireFormat.getFromUnmarshallCache(index);
            }
        } else {
            return wireFormat.unmarshalNestedObject(dataIn, bs);
        }
    }    
    protected int marshal1CachedObject(OpenWireFormat wireFormat, DataStructure o, BooleanStream bs) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            Short index = wireFormat.getMarshallCacheIndex(o);
            bs.writeBoolean(index == null);
            if( index == null ) {
                int rc = wireFormat.marshal1NestedObject(o, bs);
                wireFormat.addToMarshallCache(o);
                return 2+rc;
            } else {
                return 2;
            }
        } else {
            return wireFormat.marshal1NestedObject(o, bs);
        }
    }    
    protected void marshal2CachedObject(OpenWireFormat wireFormat, DataStructure o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            Short index = wireFormat.getMarshallCacheIndex(o);
            if( bs.readBoolean() ) {
                dataOut.writeShort(index.shortValue());
                wireFormat.marshal2NestedObject(o, dataOut, bs);
            } else {
                dataOut.writeShort(index.shortValue());
            }
        } else {
            wireFormat.marshal2NestedObject(o, dataOut, bs);
        }
    }
    
    protected Throwable unmarsalThrowable(OpenWireFormat wireFormat, DataInputStream dataIn, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            String clazz =  readString(dataIn, bs);
            String message = readString(dataIn, bs);
            Throwable o = createThrowable(clazz, message);
            if( wireFormat.isStackTraceEnabled() ) {
                if( STACK_TRACE_ELEMENT_CONSTRUCTOR!=null) {
                    StackTraceElement ss[] = new StackTraceElement[dataIn.readShort()];
                    for (int i = 0; i < ss.length; i++) {
                        try {
                            ss[i] = (StackTraceElement) STACK_TRACE_ELEMENT_CONSTRUCTOR.newInstance(new Object[]{
                                readString(dataIn, bs),
                                readString(dataIn, bs), 
                                readString(dataIn, bs), 
                                new Integer(dataIn.readInt())
                                });
                        } catch (IOException e) {
                            throw e;
                        } catch (Throwable e) {
                        }
                    }
                    o.setStackTrace(ss);
                } else {
                    int size = dataIn.readInt();
                    for (int i = 0; i < size; i++) {
                        readString(dataIn, bs);
                        readString(dataIn, bs); 
                        readString(dataIn, bs); 
                        dataIn.readInt();
                    }
                }
                o.initCause(unmarsalThrowable(wireFormat, dataIn, bs));
                
            }
            return o;
        } else {
            return null;
        }
    }
    
    private Throwable createThrowable(String className, String message) {
        try {
            Class clazz = ClassLoading.loadClass(className, DataStreamMarshaller.class.getClassLoader());
            Constructor constructor = clazz.getConstructor(new Class[]{String.class});
            return (Throwable) constructor.newInstance(new Object[]{message});
        } catch (Throwable e) {
            return new Throwable(className+": "+message);
        }
    }
    
    protected int marshalThrowable(OpenWireFormat wireFormat, Throwable o, BooleanStream bs) throws IOException { 
        if( o==null ) {
            bs.writeBoolean(false);
            return 0;
        } else {
            int rc=0;
            bs.writeBoolean(true);
            rc += writeString(o.getClass().getName(), bs);
            rc += writeString(o.getMessage(), bs);
            if( wireFormat.isStackTraceEnabled() ) {
                rc += 2;
                StackTraceElement[] stackTrace = o.getStackTrace();
                for (int i = 0; i < stackTrace.length; i++) {
                    StackTraceElement element = stackTrace[i];
                    rc += writeString(element.getClassName(), bs);
                    rc += writeString(element.getMethodName(), bs);
                    rc += writeString(element.getFileName(), bs);
                    rc += 4;
                }
                rc += marshalThrowable(wireFormat, o.getCause(), bs);
            }
            return rc;
        }
    }
    
    protected void marshalThrowable(OpenWireFormat wireFormat, Throwable o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            writeString(o.getClass().getName(), dataOut, bs);
            writeString(o.getMessage(), dataOut, bs);
            if( wireFormat.isStackTraceEnabled() ) {
                StackTraceElement[] stackTrace = o.getStackTrace();
                dataOut.writeShort(stackTrace.length);
                for (int i = 0; i < stackTrace.length; i++) {
                    StackTraceElement element = stackTrace[i];
                    writeString(element.getClassName(), dataOut, bs);
                    writeString(element.getMethodName(), dataOut, bs);
                    writeString(element.getFileName(), dataOut, bs);
                    dataOut.writeInt(element.getLineNumber());
                }
                marshalThrowable(wireFormat, o.getCause(), dataOut, bs);
            }
        }
    }
    
    protected String readString(DataInputStream dataIn, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            if( bs.readBoolean() ) {
                int size = dataIn.readShort();
                byte data[] = new byte[size];
                dataIn.readFully(data);
                return new String(data,0); // Yes deprecated, but we know what we are doing.
            }  else {
                return dataIn.readUTF();
            }
        } else {
            return null;
        }
    }
    
    protected int writeString(String value, BooleanStream bs) throws IOException { 
        bs.writeBoolean(value!=null);
        if( value!=null ) {
            
            int strlen = value.length();
            int utflen = 0;
            char[] charr = new char[strlen];
            int c, count = 0;
            boolean isOnlyAscii=true;

            value.getChars(0, strlen, charr, 0);

            for (int i = 0; i < strlen; i++) {
                c = charr[i];
                if ((c >= 0x0001) && (c <= 0x007F)) {
                    utflen++;
                } else if (c > 0x07FF) {
                    utflen += 3;
                    isOnlyAscii=false;
                } else {
                    isOnlyAscii=false;
                    utflen += 2;
                }
            }
            
            if( utflen >= Short.MAX_VALUE )
                throw new IOException("Encountered a String value that is too long to encode.");
            
            bs.writeBoolean(isOnlyAscii);            
            return utflen+2;
            
        } else {
            return 0;
        }
    }
    
    protected void writeString(String value, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            // If we verified it only holds ascii values
            if( bs.readBoolean() ) {
                dataOut.writeShort(value.length());
                dataOut.writeBytes(value);
            } else {
                dataOut.writeUTF(value);                
            }
        }
    }  
    
    protected int marshalObjectArray(OpenWireFormat wireFormat, DataStructure[] objects, BooleanStream bs) throws IOException {
        if( objects != null ) {
            int rc=0;
            bs.writeBoolean(true);
            rc += 2;
            for( int i=0; i < objects.length; i++ ) {
                rc += marshal1NestedObject(wireFormat,objects[i], bs);
            }
            return rc;
        } else {
            bs.writeBoolean(false);
            return 0;
        }
    }
    
    protected void marshalObjectArray(OpenWireFormat wireFormat, DataStructure[] objects, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            dataOut.writeShort(objects.length);
            for( int i=0; i < objects.length; i++ ) {
                marshal2NestedObject(wireFormat,objects[i], dataOut, bs);
            }
        }
    }

}
