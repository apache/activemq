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
package org.apache.activemq.protobuf;


final public class AsciiBuffer extends Buffer {

    private int hashCode;

    public AsciiBuffer(Buffer other) {
        super(other);
    }

    public AsciiBuffer(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public AsciiBuffer(byte[] data) {
        super(data);
    }

    public AsciiBuffer(String input) {
        super(encode(input));
    }

    public AsciiBuffer compact() {
        if (length != data.length) {
            return new AsciiBuffer(toByteArray());
        }
        return this;
    }

    public String toString()
    {
        return decode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if( obj==this )
            return true;
         
         if( obj==null || obj.getClass()!=AsciiBuffer.class )
            return false;
         
         return equals((Buffer)obj);
    }
    
    @Override
    public int hashCode() {
        if( hashCode==0 ) {
            hashCode = super.hashCode();;
        }
        return hashCode;
    }
    
    static public byte[] encode(String value)
    {
        int size = value.length();
        byte rc[] = new byte[size];
        for( int i=0; i < size; i++ ) {
            rc[i] = (byte)(value.charAt(i)&0xFF);
        }
        return rc;
    }
    static public String decode(Buffer value)
    {
        int size = value.getLength();
        char rc[] = new char[size];
        for( int i=0; i < size; i++ ) {
            rc[i] = (char)(value.byteAt(i) & 0xFF );
        }
        return new String(rc);
    }

    
}
