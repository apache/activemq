/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ppr/io/DataOutputStream.hpp"

using namespace apache::ppr::io;

void DataOutputStream::writeByte(char value) throw(IOException)
{
    // Write a single byte
    write((char*)&value, 0, sizeof(char)) ;
}

void DataOutputStream::writeBoolean(bool value) throw(IOException)
{
    // Write a boolean
    ( value ) ? writeByte(0x01) : writeByte(0x00) ;
}

void DataOutputStream::writeDouble(double v) throw(IOException)
{
    // Write a double, convert from little endian to big endian if necessary
    double value = htond(v) ;
    write((char*)&value, 0, sizeof(double)) ;
}

void DataOutputStream::writeFloat(float v) throw(IOException)
{
    // Write a float, convert from little endian to big endian if necessary
    float value = htonf(v) ;
    write((char*)&value, 0, sizeof(float)) ;
}

void DataOutputStream::writeShort(short v) throw(IOException)
{
    // Write a short, convert from little endian to big endian if necessary
    short value = htons(v) ;
    write((char*)&value, 0, sizeof(short)) ;
}

void DataOutputStream::writeInt(int v) throw(IOException)
{
    // Write an int, convert from little endian to big endian if necessary
    int value = htoni(v) ;
    write((char*)&value, 0, sizeof(int)) ;
}

void DataOutputStream::writeLong(long long v) throw(IOException)
{
    // Write a long long, convert from little endian to big endian if necessary
    long long value = htonll(v) ;
    write((char*)&value, 0, sizeof(long long)) ;
}

void DataOutputStream::writeString(p<string> value) throw(IOException)
{
    // Assert argument
    if( value == NULL )
        return ;
    if( value->length() > USHRT_MAX )
        throw IOException("String length exceeds maximum length") ;

    // Write length of string
    short length = (short)value->length() ;
    writeShort( length ) ;

    // Write string contents
    write((char*)value->c_str(), 0, length) ;
}
