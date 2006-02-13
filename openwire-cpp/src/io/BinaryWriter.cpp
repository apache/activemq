/* 
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "io/BinaryWriter.hpp"

using namespace apache::activemq::client::io;

/*
 *
 */
BinaryWriter::BinaryWriter()
{
    // no-op
}

/*
 *
 */
BinaryWriter::~BinaryWriter()
{
    // no-op
}

void BinaryWriter::writeByte(char value) throw(IOException)
{
    // Write a single byte
    write((char*)&value, sizeof(char)) ;
}

void BinaryWriter::writeBoolean(bool value) throw(IOException)
{
    // Write a boolean
    write((char*)&value, sizeof(bool)) ;
}

void BinaryWriter::writeDouble(double v) throw(IOException)
{
    // Write a double, convert from little endian to big endian if necessary
    double value = htond(v) ;
    write((char*)&value, sizeof(double)) ;
}

void BinaryWriter::writeFloat(float v) throw(IOException)
{
    // Write a float, convert from little endian to big endian if necessary
    float value = htonf(v) ;
    write((char*)&value, sizeof(float)) ;
}

void BinaryWriter::writeShort(short v) throw(IOException)
{
    // Write a short, convert from little endian to big endian if necessary
    short value = htons(v) ;
    write((char*)&value, sizeof(short)) ;
}

void BinaryWriter::writeInt(int v) throw(IOException)
{
    // Write an int, convert from little endian to big endian if necessary
    int value = htoni(v) ;
    write((char*)&value, sizeof(int)) ;
}

void BinaryWriter::writeLong(long long v) throw(IOException)
{
    // Write a long long, convert from little endian to big endian if necessary
    long long value = htonl(v) ;
    write((char*)&value, sizeof(long long)) ;
}

void BinaryWriter::writeString(p<string> value) throw(IOException)
{
    // Assert argument
    if( value->length() > USHRT_MAX )
        throw IOException("String length exceeds maximum length") ;

    // Write length of string
    short length = (short)value->length() ;
    writeShort( length ) ;

    // Write string contents
    write((char*)value->c_str(), length) ;
}
