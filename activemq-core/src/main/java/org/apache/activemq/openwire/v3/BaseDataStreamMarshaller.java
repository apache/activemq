/*
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
package org.apache.activemq.openwire.v3;

import org.apache.activemq.openwire.DataStreamMarshaller;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.openwire.BooleanStream;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.util.ClassLoading;
import org.apache.activemq.util.ByteSequence;

import java.lang.reflect.Constructor;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

abstract public class BaseDataStreamMarshaller implements DataStreamMarshaller {

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

    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {
        return 0;
    }
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutput dataOut, BooleanStream bs) throws IOException {
    }
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn, BooleanStream bs) throws IOException {
    }

    public int tightMarshalLong1(OpenWireFormat wireFormat, long o, BooleanStream bs) throws IOException {
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
    public void tightMarshalLong2(OpenWireFormat wireFormat, long o, DataOutput dataOut, BooleanStream bs) throws IOException {
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
    public long tightUnmarshalLong(OpenWireFormat wireFormat, DataInput dataIn, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            if( bs.readBoolean() ) {
                return dataIn.readLong();
            } else {
                return toLong(dataIn.readInt());
            }
        } else {
            if( bs.readBoolean() ) {
                return toLong(dataIn.readShort());
            } else {
                return 0;
            }
        }
    }

    protected long toLong(short value) {
        // lets handle negative values
        long answer = value;
        return answer & 0xffffL;
    }

    protected long toLong(int value) {
        // lets handle negative values
        long answer = value;
        return answer & 0xffffffffL;
    }

    protected DataStructure tightUnmarsalNestedObject(OpenWireFormat wireFormat, DataInput dataIn, BooleanStream bs) throws IOException {
        return wireFormat.tightUnmarshalNestedObject(dataIn, bs);
    }
    protected int tightMarshalNestedObject1(OpenWireFormat wireFormat, DataStructure o, BooleanStream bs) throws IOException {
        return wireFormat.tightMarshalNestedObject1(o, bs);
    }

    protected void tightMarshalNestedObject2(OpenWireFormat wireFormat, DataStructure o, DataOutput dataOut, BooleanStream bs) throws IOException {
        wireFormat.tightMarshalNestedObject2(o, dataOut, bs);
    }

    protected DataStructure tightUnmarsalCachedObject(OpenWireFormat wireFormat, DataInput dataIn, BooleanStream bs) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            if( bs.readBoolean() ) {
                short index = dataIn.readShort();
                DataStructure object = wireFormat.tightUnmarshalNestedObject(dataIn, bs);
                wireFormat.setInUnmarshallCache(index, object);
                return object;
            } else {
                short index = dataIn.readShort();
                return wireFormat.getFromUnmarshallCache(index);
            }
        } else {
            return wireFormat.tightUnmarshalNestedObject(dataIn, bs);
        }
    }
    protected int tightMarshalCachedObject1(OpenWireFormat wireFormat, DataStructure o, BooleanStream bs) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            Short index = wireFormat.getMarshallCacheIndex(o);
            bs.writeBoolean(index == null);
            if( index == null ) {
                int rc = wireFormat.tightMarshalNestedObject1(o, bs);
                wireFormat.addToMarshallCache(o);
                return 2+rc;
            } else {
                return 2;
            }
        } else {
            return wireFormat.tightMarshalNestedObject1(o, bs);
        }
    }
    protected void tightMarshalCachedObject2(OpenWireFormat wireFormat, DataStructure o, DataOutput dataOut, BooleanStream bs) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            Short index = wireFormat.getMarshallCacheIndex(o);
            if( bs.readBoolean() ) {
                dataOut.writeShort(index.shortValue());
                wireFormat.tightMarshalNestedObject2(o, dataOut, bs);
            } else {
                dataOut.writeShort(index.shortValue());
            }
        } else {
            wireFormat.tightMarshalNestedObject2(o, dataOut, bs);
        }
    }

    protected Throwable tightUnmarsalThrowable(OpenWireFormat wireFormat, DataInput dataIn, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            String clazz =  tightUnmarshalString(dataIn, bs);
            String message = tightUnmarshalString(dataIn, bs);
            Throwable o = createThrowable(clazz, message);
            if( wireFormat.isStackTraceEnabled() ) {
                if( STACK_TRACE_ELEMENT_CONSTRUCTOR !=null) {
                    StackTraceElement ss[] = new StackTraceElement[dataIn.readShort()];
                    for (int i = 0; i < ss.length; i++) {
                        try {
                            ss[i] = (StackTraceElement) STACK_TRACE_ELEMENT_CONSTRUCTOR.newInstance(new Object[]{
                                tightUnmarshalString(dataIn, bs),
                                tightUnmarshalString(dataIn, bs),
                                tightUnmarshalString(dataIn, bs),
                                Integer.valueOf(dataIn.readInt())
                                });
                        } catch (IOException e) {
                            throw e;
                        } catch (Throwable e) {
                        }
                    }
                    o.setStackTrace(ss);
                } else {
                    short size = dataIn.readShort();
                    for (int i = 0; i < size; i++) {
                        tightUnmarshalString(dataIn, bs);
                        tightUnmarshalString(dataIn, bs);
                        tightUnmarshalString(dataIn, bs);
                        dataIn.readInt();
                    }
                }
                o.initCause(tightUnmarsalThrowable(wireFormat, dataIn, bs));

            }
            return o;
        } else {
            return null;
        }
    }

    private Throwable createThrowable(String className, String message) {
        try {
            Class clazz = ClassLoading.loadClass(className, BaseDataStreamMarshaller.class.getClassLoader());
            Constructor constructor = clazz.getConstructor(new Class[]{String.class});
            return (Throwable) constructor.newInstance(new Object[]{message});
        } catch (Throwable e) {
            return new Throwable(className+": "+message);
        }
    }

    protected int tightMarshalThrowable1(OpenWireFormat wireFormat, Throwable o, BooleanStream bs) throws IOException {
        if( o==null ) {
            bs.writeBoolean(false);
            return 0;
        } else {
            int rc=0;
            bs.writeBoolean(true);
            rc += tightMarshalString1(o.getClass().getName(), bs);
            rc += tightMarshalString1(o.getMessage(), bs);
            if( wireFormat.isStackTraceEnabled() ) {
                rc += 2;
                StackTraceElement[] stackTrace = o.getStackTrace();
                for (int i = 0; i < stackTrace.length; i++) {
                    StackTraceElement element = stackTrace[i];
                    rc += tightMarshalString1(element.getClassName(), bs);
                    rc += tightMarshalString1(element.getMethodName(), bs);
                    rc += tightMarshalString1(element.getFileName(), bs);
                    rc += 4;
                }
                rc += tightMarshalThrowable1(wireFormat, o.getCause(), bs);
            }
            return rc;
        }
    }

    protected void tightMarshalThrowable2(OpenWireFormat wireFormat, Throwable o, DataOutput dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            tightMarshalString2(o.getClass().getName(), dataOut, bs);
            tightMarshalString2(o.getMessage(), dataOut, bs);
            if( wireFormat.isStackTraceEnabled() ) {
                StackTraceElement[] stackTrace = o.getStackTrace();
                dataOut.writeShort(stackTrace.length);
                for (int i = 0; i < stackTrace.length; i++) {
                    StackTraceElement element = stackTrace[i];
                    tightMarshalString2(element.getClassName(), dataOut, bs);
                    tightMarshalString2(element.getMethodName(), dataOut, bs);
                    tightMarshalString2(element.getFileName(), dataOut, bs);
                    dataOut.writeInt(element.getLineNumber());
                }
                tightMarshalThrowable2(wireFormat, o.getCause(), dataOut, bs);
            }
        }
    }

    protected String tightUnmarshalString(DataInput dataIn, BooleanStream bs) throws IOException {
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

    protected int tightMarshalString1(String value, BooleanStream bs) throws IOException {
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

    protected void tightMarshalString2(String value, DataOutput dataOut, BooleanStream bs) throws IOException {
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

    protected int tightMarshalObjectArray1(OpenWireFormat wireFormat, DataStructure[] objects, BooleanStream bs) throws IOException {
        if( objects != null ) {
            int rc=0;
            bs.writeBoolean(true);
            rc += 2;
            for( int i=0; i < objects.length; i++ ) {
                rc += tightMarshalNestedObject1(wireFormat,objects[i], bs);
            }
            return rc;
        } else {
            bs.writeBoolean(false);
            return 0;
        }
    }

    protected void tightMarshalObjectArray2(OpenWireFormat wireFormat, DataStructure[] objects, DataOutput dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ) {
            dataOut.writeShort(objects.length);
            for( int i=0; i < objects.length; i++ ) {
                tightMarshalNestedObject2(wireFormat,objects[i], dataOut, bs);
            }
        }
    }

    protected int tightMarshalConstByteArray1(byte[] data, BooleanStream bs, int i) throws IOException {
        return i;
    }
    protected void tightMarshalConstByteArray2(byte[] data, DataOutput dataOut, BooleanStream bs, int i) throws IOException {
        dataOut.write(data, 0, i);
    }

    protected byte[] tightUnmarshalConstByteArray(DataInput dataIn, BooleanStream bs, int i) throws IOException {
        byte data[] = new byte[i];
        dataIn.readFully(data);
        return data;
    }

    protected int tightMarshalByteArray1(byte[] data, BooleanStream bs) throws IOException {
        bs.writeBoolean(data!=null);
        if( data!=null ){
            return data.length+4;
        } else {
            return 0;
        }
    }

    protected void tightMarshalByteArray2(byte[] data, DataOutput dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ){
            dataOut.writeInt(data.length);
            dataOut.write(data);
        }
    }

    protected byte[] tightUnmarshalByteArray(DataInput dataIn, BooleanStream bs) throws IOException {
        byte rc[]=null;
        if( bs.readBoolean() ) {
            int size = dataIn.readInt();
            rc = new byte[size];
            dataIn.readFully(rc);
        }
        return rc;
    }

    protected int tightMarshalByteSequence1(ByteSequence data, BooleanStream bs) throws IOException {
        bs.writeBoolean(data!=null);
        if( data!=null ){
            return data.getLength()+4;
        } else {
            return 0;
        }
    }

    protected void tightMarshalByteSequence2(ByteSequence data, DataOutput dataOut, BooleanStream bs) throws IOException {
        if( bs.readBoolean() ){
            dataOut.writeInt(data.getLength());
            dataOut.write(data.getData(), data.getOffset(), data.getLength());
        }
    }

    protected ByteSequence tightUnmarshalByteSequence(DataInput dataIn, BooleanStream bs) throws IOException {
        ByteSequence rc=null;
        if( bs.readBoolean() ) {
            int size = dataIn.readInt();
            byte[] t = new byte[size];
            dataIn.readFully(t);
            return new ByteSequence(t, 0, size);
        }
        return rc;
    }

    //
    // The loose marshaling logic
    //

    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {
    }
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn) throws IOException {
    }

    public void looseMarshalLong(OpenWireFormat wireFormat, long o, DataOutput dataOut) throws IOException {
        dataOut.writeLong(o);
    }
    public long looseUnmarshalLong(OpenWireFormat wireFormat, DataInput dataIn) throws IOException {
        return dataIn.readLong();
    }

    protected DataStructure looseUnmarsalNestedObject(OpenWireFormat wireFormat, DataInput dataIn) throws IOException {
        return wireFormat.looseUnmarshalNestedObject(dataIn);
    }
    protected void looseMarshalNestedObject(OpenWireFormat wireFormat, DataStructure o, DataOutput dataOut) throws IOException {
        wireFormat.looseMarshalNestedObject(o, dataOut);
    }

    protected DataStructure looseUnmarsalCachedObject(OpenWireFormat wireFormat, DataInput dataIn) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            if( dataIn.readBoolean() ) {
                short index = dataIn.readShort();
                DataStructure object = wireFormat.looseUnmarshalNestedObject(dataIn);
                wireFormat.setInUnmarshallCache(index, object);
                return object;
            } else {
                short index = dataIn.readShort();
                return wireFormat.getFromUnmarshallCache(index);
            }
        } else {
            return wireFormat.looseUnmarshalNestedObject(dataIn);
        }
    }
    protected void looseMarshalCachedObject(OpenWireFormat wireFormat, DataStructure o, DataOutput dataOut) throws IOException {
        if( wireFormat.isCacheEnabled() ) {
            Short index = wireFormat.getMarshallCacheIndex(o);
            dataOut.writeBoolean(index == null);
            if( index == null ) {
                index = wireFormat.addToMarshallCache(o);
                dataOut.writeShort(index.shortValue());
                wireFormat.looseMarshalNestedObject(o, dataOut);
            } else {
                dataOut.writeShort(index.shortValue());
            }
        } else {
            wireFormat.looseMarshalNestedObject(o, dataOut);
        }
    }

    protected Throwable looseUnmarsalThrowable(OpenWireFormat wireFormat, DataInput dataIn) throws IOException {
        if( dataIn.readBoolean() ) {
            String clazz =  looseUnmarshalString(dataIn);
            String message = looseUnmarshalString(dataIn);
            Throwable o = createThrowable(clazz, message);
            if( wireFormat.isStackTraceEnabled() ) {
                if( STACK_TRACE_ELEMENT_CONSTRUCTOR !=null) {
                    StackTraceElement ss[] = new StackTraceElement[dataIn.readShort()];
                    for (int i = 0; i < ss.length; i++) {
                        try {
                            ss[i] = (StackTraceElement) STACK_TRACE_ELEMENT_CONSTRUCTOR.newInstance(new Object[]{
                                looseUnmarshalString(dataIn),
                                looseUnmarshalString(dataIn),
                                looseUnmarshalString(dataIn),
                                Integer.valueOf(dataIn.readInt())
                                });
                        } catch (IOException e) {
                            throw e;
                        } catch (Throwable e) {
                        }
                    }
                    o.setStackTrace(ss);
                } else {
                    short size = dataIn.readShort();
                    for (int i = 0; i < size; i++) {
                        looseUnmarshalString(dataIn);
                        looseUnmarshalString(dataIn);
                        looseUnmarshalString(dataIn);
                        dataIn.readInt();
                    }
                }
                o.initCause(looseUnmarsalThrowable(wireFormat, dataIn));

            }
            return o;
        } else {
            return null;
        }
    }


    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o, DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(o!=null);
        if( o!=null ) {
            looseMarshalString(o.getClass().getName(), dataOut);
            looseMarshalString(o.getMessage(), dataOut);
            if( wireFormat.isStackTraceEnabled() ) {
                StackTraceElement[] stackTrace = o.getStackTrace();
                dataOut.writeShort(stackTrace.length);
                for (int i = 0; i < stackTrace.length; i++) {
                    StackTraceElement element = stackTrace[i];
                    looseMarshalString(element.getClassName(), dataOut);
                    looseMarshalString(element.getMethodName(), dataOut);
                    looseMarshalString(element.getFileName(), dataOut);
                    dataOut.writeInt(element.getLineNumber());
                }
                looseMarshalThrowable(wireFormat, o.getCause(), dataOut);
            }
        }
    }

    protected String looseUnmarshalString(DataInput dataIn) throws IOException {
        if( dataIn.readBoolean() ) {
            return dataIn.readUTF();
        } else {
            return null;
        }
    }

    protected void looseMarshalString(String value, DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(value!=null);
        if( value!=null ) {
            dataOut.writeUTF(value);
        }
    }

    protected void looseMarshalObjectArray(OpenWireFormat wireFormat, DataStructure[] objects, DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(objects!=null);
        if( objects!=null ) {
            dataOut.writeShort(objects.length);
            for( int i=0; i < objects.length; i++ ) {
                looseMarshalNestedObject(wireFormat,objects[i], dataOut);
            }
        }
    }

    protected void looseMarshalConstByteArray(OpenWireFormat wireFormat, byte[] data, DataOutput dataOut, int i) throws IOException {
        dataOut.write(data, 0, i);
    }

    protected byte[] looseUnmarshalConstByteArray(DataInput dataIn, int i) throws IOException {
        byte data[] = new byte[i];
        dataIn.readFully(data);
        return data;
    }

    protected void looseMarshalByteArray(OpenWireFormat wireFormat, byte[] data, DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(data!=null);
        if( data!=null ){
            dataOut.writeInt(data.length);
            dataOut.write(data);
        }
    }

    protected byte[] looseUnmarshalByteArray(DataInput dataIn) throws IOException {
        byte rc[]=null;
        if( dataIn.readBoolean() ) {
            int size = dataIn.readInt();
            rc = new byte[size];
            dataIn.readFully(rc);
        }
        return rc;
    }

    protected void looseMarshalByteSequence(OpenWireFormat wireFormat, ByteSequence data, DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(data!=null);
        if( data!=null ){
            dataOut.writeInt(data.getLength());
            dataOut.write(data.getData(), data.getOffset(), data.getLength());
        }
    }

    protected ByteSequence looseUnmarshalByteSequence(DataInput dataIn) throws IOException {
        ByteSequence rc=null;
        if( dataIn.readBoolean() ) {
            int size = dataIn.readInt();
            byte[] t = new byte[size];
            dataIn.readFully(t);
            rc = new ByteSequence(t, 0, size);
        }
        return rc;
    }
}
