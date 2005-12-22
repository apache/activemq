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
package org.activemq.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.HashMap;
import java.util.Iterator;

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

    static  public void marshalPrimitiveMap(HashMap map, DataOutputStream out) throws IOException {
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

    /**
     * @param in
     * @return
     * @throws IOException
     */
    static public HashMap unmarshalPrimitiveMap(DataInputStream in) throws IOException {
        int size = in.readInt();
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

    static public void marshalPrimitive(DataOutputStream out, Object value) throws IOException {
        if( value == null ) {
            out.writeByte(NULL);
        } else if( value.getClass() == Boolean.class ) {
            out.writeByte(BOOLEAN_TYPE);
            out.writeBoolean(((Boolean)value).booleanValue());
        } else if( value.getClass() == Byte.class ) {
            out.writeByte(BYTE_TYPE);
            out.writeByte(((Byte)value).byteValue());
        } else if( value.getClass() == Character.class ) {
            out.writeByte(CHAR_TYPE);
            out.writeChar(((Character)value).charValue());
        } else if( value.getClass() == Short.class ) {
            out.writeByte(SHORT_TYPE);
            out.writeShort(((Short)value).shortValue());
        } else if( value.getClass() == Integer.class ) {
            out.writeByte(INTEGER_TYPE);
            out.writeInt(((Integer)value).intValue());
        } else if( value.getClass() == Long.class ) {
            out.writeByte(LONG_TYPE);
            out.writeLong(((Long)value).longValue());
        } else if( value.getClass() == Float.class ) {
            out.writeByte(FLOAT_TYPE);
            out.writeFloat(((Float)value).floatValue());
        } else if( value.getClass() == Double.class ) {
            out.writeByte(DOUBLE_TYPE);
            out.writeDouble(((Double)value).doubleValue());
        } else if( value.getClass() == byte[].class ) {
            out.writeByte(BYTE_ARRAY_TYPE);
            out.writeInt(((byte[])value).length);
            out.write(((byte[])value));
        } else if( value.getClass() == String.class ) {
            out.writeByte(STRING_TYPE);
            out.writeUTF((String)value);
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
        }
        return value;
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

}
