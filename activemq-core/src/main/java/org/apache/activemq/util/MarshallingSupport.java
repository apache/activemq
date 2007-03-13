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
package org.apache.activemq.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 
 * The fixed version of the UTF8 encoding function.  Some older JVM's UTF8 encoding function
 * breaks when handling large strings. 
 * 
 * @version $Revision$
 */
public class MarshallingSupport {
   
    public static final byte NULL                    = 0;
    public static final byte BOOLEAN_TYPE            = 1;
    public static final byte BYTE_TYPE               = 2;
    public static final byte CHAR_TYPE               = 3;
    public static final byte SHORT_TYPE              = 4;
    public static final byte INTEGER_TYPE            = 5;
    public static final byte LONG_TYPE               = 6;
    public static final byte DOUBLE_TYPE             = 7;
    public static final byte FLOAT_TYPE              = 8;
    public static final byte STRING_TYPE             = 9;
    public static final byte BYTE_ARRAY_TYPE         = 10;
    public static final byte MAP_TYPE                = 11;
    public static final byte LIST_TYPE               = 12;
    public static final byte BIG_STRING_TYPE         = 13;

    static  public void marshalPrimitiveMap(Map map, DataOutputStream out) throws IOException {
        if( map == null ) {
            out.writeInt(-1);
        } else {
            out.writeInt(map.size());
            for (Iterator iter = map.keySet().iterator(); iter.hasNext();) {
                String name = (String) iter.next();
                out.writeUTF(name);
                Object value = map.get(name);
                marshalPrimitive(out, value);
            }
        }
    }

    static public Map unmarshalPrimitiveMap(DataInputStream in) throws IOException {
		return unmarshalPrimitiveMap(in, Integer.MAX_VALUE);
	}

    /**
     * @param in
     * @return
     * @throws IOException 
     * @throws IOException
     */
	public static Map unmarshalPrimitiveMap(DataInputStream in, int max_property_size) throws IOException {
        int size = in.readInt();
        if( size > max_property_size ) {
        	throw new IOException("Primitive map is larger than the allowed size: "+size);
        }
        if( size < 0 ) {
            return null;
        } else {
            HashMap rc = new HashMap(size);
            for(int i=0; i < size; i++) {
                String name = in.readUTF();
                rc.put(name, unmarshalPrimitive(in));
            }
            return rc;
        }
        
    }

    public static void marshalPrimitiveList(List list, DataOutputStream out) throws IOException {
        out.writeInt(list.size());
        for (Iterator iter = list.iterator(); iter.hasNext();) {
            Object element = (Object) iter.next();
            marshalPrimitive(out, element);
        }
    }

    public static List unmarshalPrimitiveList(DataInputStream in) throws IOException {
        int size = in.readInt();
        List answer = new ArrayList(size);
        while (size-- > 0) {
            answer.add(unmarshalPrimitive(in));
        }
        return answer;
    }

    static public void marshalPrimitive(DataOutputStream out, Object value) throws IOException {
        if( value == null ) {
            marshalNull(out);
        } else if( value.getClass() == Boolean.class ) {
            marshalBoolean(out, ((Boolean)value).booleanValue());
        } else if( value.getClass() == Byte.class ) {
            marshalByte(out, ((Byte)value).byteValue());
        } else if( value.getClass() == Character.class ) {
            marshalChar(out, ((Character)value).charValue());
        } else if( value.getClass() == Short.class ) {
            marshalShort(out, ((Short)value).shortValue());
        } else if( value.getClass() == Integer.class ) {
            marshalInt(out, ((Integer)value).intValue());
        } else if( value.getClass() == Long.class ) {
            marshalLong(out, ((Long)value).longValue());
        } else if( value.getClass() == Float.class ) {
            marshalFloat(out, ((Float)value).floatValue());
        } else if( value.getClass() == Double.class ) {
            marshalDouble(out, ((Double)value).doubleValue());
        } else if( value.getClass() == byte[].class ) {
            marshalByteArray(out, ((byte[])value));
        } else if( value.getClass() == String.class ) {
            marshalString(out, (String)value);
        } else if( value instanceof Map) {
            out.writeByte(MAP_TYPE);
            marshalPrimitiveMap((Map) value, out);
        } else if( value instanceof List) {
            out.writeByte(LIST_TYPE);
            marshalPrimitiveList((List) value, out);
        } else {
            throw new IOException("Object is not a primitive: "+value);
        }
    }


    static public Object unmarshalPrimitive(DataInputStream in) throws IOException {
        Object value=null;
        switch( in.readByte() ) {
        case BYTE_TYPE:
            value = new Byte(in.readByte());
            break;
        case BOOLEAN_TYPE:
            value = in.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            break;
        case CHAR_TYPE:
            value = new Character(in.readChar());
            break;
        case SHORT_TYPE:
            value = new Short(in.readShort());
            break;
        case INTEGER_TYPE:
            value = new Integer(in.readInt());
            break;
        case LONG_TYPE:
            value = new Long(in.readLong());
            break;
        case FLOAT_TYPE:
            value = new Float(in.readFloat());
            break;
        case DOUBLE_TYPE:
            value = new Double(in.readDouble());
            break;
        case BYTE_ARRAY_TYPE:
            value = new byte[in.readInt()];
            in.readFully((byte[])value);
            break;
        case STRING_TYPE:
            value = in.readUTF();
            break;
        case BIG_STRING_TYPE:
            value = readUTF8(in);
            break;
        case MAP_TYPE:
            value = unmarshalPrimitiveMap(in);
            break;
        case LIST_TYPE:
            value = unmarshalPrimitiveList(in);
            break;
        }
        return value;
    }

    public static void marshalNull(DataOutputStream out) throws IOException {
        out.writeByte(NULL);
    }

    public static void marshalBoolean(DataOutputStream out, boolean value) throws IOException {
        out.writeByte(BOOLEAN_TYPE);
        out.writeBoolean(value);
    }

    public static void marshalByte(DataOutputStream out, byte value) throws IOException {
        out.writeByte(BYTE_TYPE);
        out.writeByte(value);
    }

    public static void marshalChar(DataOutputStream out, char value) throws IOException {
        out.writeByte(CHAR_TYPE);
        out.writeChar(value);
    }

    public static void marshalShort(DataOutputStream out, short value) throws IOException {
        out.writeByte(SHORT_TYPE);
        out.writeShort(value);
    }

    public static void marshalInt(DataOutputStream out, int value) throws IOException {
        out.writeByte(INTEGER_TYPE);
        out.writeInt(value);
    }

    public static void marshalLong(DataOutputStream out, long value) throws IOException {
        out.writeByte(LONG_TYPE);
        out.writeLong(value);
    }

    public static void marshalFloat(DataOutputStream out, float value) throws IOException {
        out.writeByte(FLOAT_TYPE);
        out.writeFloat(value);
    }

    public static void marshalDouble(DataOutputStream out, double value) throws IOException {
        out.writeByte(DOUBLE_TYPE);
        out.writeDouble(value);
    }

    public static void marshalByteArray(DataOutputStream out, byte[] value) throws IOException {
        marshalByteArray(out, value, 0, value.length);
    }

    public static void marshalByteArray(DataOutputStream out, byte[] value, int offset, int length) throws IOException {
        out.writeByte(BYTE_ARRAY_TYPE);
        out.writeInt(length);
        out.write(value, offset, length);
    }


    public static void marshalString(DataOutputStream out, String s) throws IOException {
        // If it's too big, out.writeUTF may not able able to write it out.
        if( s.length() < Short.MAX_VALUE/4 ) {
            out.writeByte(STRING_TYPE);
            out.writeUTF(s);
        } else {
            out.writeByte(BIG_STRING_TYPE);
            writeUTF8(out, s);
        }
    }

    static public void writeUTF8(DataOutput dataOut, String text) throws IOException {
        if (text != null) {
            int strlen = text.length();
            int utflen = 0;
            char[] charr = new char[strlen];
            int c, count = 0;

            text.getChars(0, strlen, charr, 0);

            for (int i = 0; i < strlen; i++) {
                c = charr[i];
                if ((c >= 0x0001) && (c <= 0x007F)) {
                    utflen++;
                } else if (c > 0x07FF) {
                    utflen += 3;
                } else {
                    utflen += 2;
                }
            }
            //TODO diff: Sun code - removed
            byte[] bytearr = new byte[utflen + 4]; //TODO diff: Sun code
            bytearr[count++] = (byte) ((utflen >>> 24) & 0xFF); //TODO diff: Sun code
            bytearr[count++] = (byte) ((utflen >>> 16) & 0xFF); //TODO diff: Sun code
            bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
            bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);
            for (int i = 0; i < strlen; i++) {
                c = charr[i];
                if ((c >= 0x0001) && (c <= 0x007F)) {
                    bytearr[count++] = (byte) c;
                } else if (c > 0x07FF) {
                    bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                    bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                    bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                } else {
                    bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                    bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                }
            }
            dataOut.write(bytearr);

        } else {
            dataOut.writeInt(-1);
        }
    }

    static public String readUTF8(DataInput dataIn) throws IOException {
        int utflen = dataIn.readInt(); //TODO diff: Sun code
        if (utflen > -1) {
            StringBuffer str = new StringBuffer(utflen);
            byte bytearr[] = new byte[utflen];
            int c, char2, char3;
            int count = 0;

            dataIn.readFully(bytearr, 0, utflen);

            while (count < utflen) {
                c = bytearr[count] & 0xff;
                switch (c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        /* 0xxxxxxx */
                        count++;
                        str.append((char) c);
                        break;
                    case 12:
                    case 13:
                        /* 110x xxxx 10xx xxxx */
                        count += 2;
                        if (count > utflen) {
                            throw new UTFDataFormatException();
                        }
                        char2 = bytearr[count - 1];
                        if ((char2 & 0xC0) != 0x80) {
                            throw new UTFDataFormatException();
                        }
                        str.append((char) (((c & 0x1F) << 6) | (char2 & 0x3F)));
                        break;
                    case 14:
                        /* 1110 xxxx 10xx xxxx 10xx xxxx */
                        count += 3;
                        if (count > utflen) {
                            throw new UTFDataFormatException();
                        }
                        char2 = bytearr[count - 2]; //TODO diff: Sun code
                        char3 = bytearr[count - 1]; //TODO diff: Sun code
                        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                            throw new UTFDataFormatException();
                        }
                        str.append((char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0)));
                        break;
                    default :
                        /* 10xx xxxx, 1111 xxxx */
                        throw new UTFDataFormatException();
                }
            }
            // The number of chars produced may be less than utflen
            return new String(str);
        } else {
            return null;
        }
    }
    
    public static String propertiesToString(Properties props) throws IOException{
        String result="";
        if(props!=null){
            DataByteArrayOutputStream dataOut=new DataByteArrayOutputStream();
            props.store(dataOut,"");
            result=new String(dataOut.getData(),0,dataOut.size());
        }
        return result;
    }
    
    public static Properties stringToProperties(String str) throws IOException {
        Properties result = new Properties();
        if (str != null && str.length() > 0 ) {
            DataByteArrayInputStream dataIn = new DataByteArrayInputStream(str.getBytes());
            result.load(dataIn);
        }
        return result;
    }


}
