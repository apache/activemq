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

import java.io.UnsupportedEncodingException;

final public class UTF8Buffer extends Buffer {

    int hashCode;
    String value; 
    
    public UTF8Buffer(Buffer other) {
        super(other);
    }

    public UTF8Buffer(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public UTF8Buffer(byte[] data) {
        super(data);
    }

    public UTF8Buffer(String input) {
        super(encode(input));
    }

    public UTF8Buffer compact() {
        if (length != data.length) {
            return new UTF8Buffer(toByteArray());
        }
        return this;
    }

    public String toString()
    {
        if( value==null ) {
            value = decode(this); 
        }
        return value;
    }
    
    @Override
    public int compareTo(Buffer other) {
        // Do a char comparison.. not a byte for byte comparison.
        return toString().compareTo(other.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if( obj==this )
            return true;
         
         if( obj==null || obj.getClass()!=UTF8Buffer.class )
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
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("A UnsupportedEncodingException was thrown for teh UTF-8 encoding. (This should never happen)");
        }
    }

    static public String decode(Buffer buffer)
    {
        try {
            return new String(buffer.getData(), buffer.getOffset(), buffer.getLength(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("A UnsupportedEncodingException was thrown for teh UTF-8 encoding. (This should never happen)");
        }
    }
    

}
