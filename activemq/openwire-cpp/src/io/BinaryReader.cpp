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
#include "io/BinaryReader.hpp"

using namespace apache::activemq::client::io;

/*
 *
 */
BinaryReader::BinaryReader()
{
    // no-op
}

/*
 *
 */
BinaryReader::~BinaryReader()
{
    // no-op
}

/*
 *
 */
char BinaryReader::readByte() throw(IOException)
{
    char value ;

    // Read a single byte
    read(&value, sizeof(char)) ;
    return value ;
}

/*
 *
 */
bool BinaryReader::readBoolean() throw(IOException)
{
    bool value ;

    // Read a boolean
    read((char*)&value, sizeof(bool)) ;
    return value ;
}

/*
 *
 */
double BinaryReader::readDouble() throw(IOException)
{
    double value ;

    // Read a double and convert from big endian to little endian if necessary
    read((char*)&value, sizeof(double) ) ;
    return ntohd(value) ;
}

/*
 *
 */
float BinaryReader::readFloat() throw(IOException)
{
    float value ;

    // Read a float and convert from big endian to little endian if necessary
    read((char*)&value, sizeof(float)) ;
    return ntohf(value) ;
}

/*
 *
 */
short BinaryReader::readShort() throw(IOException)
{
    short value ;

    // Read a short and convert from big endian to little endian if necessary
    read((char*)&value, sizeof(short)) ;
    return ntohs(value) ;
}

/*
 *
 */
int BinaryReader::readInt() throw(IOException)
{
    int value ;

    // Read an int and convert from big endian to little endian if necessary
    read((char*)&value, sizeof(int)) ;
    return ntohi(value) ;
}

/*
 *
 */
long long BinaryReader::readLong() throw(IOException)
{
    long long value ;

    // Read a long long and convert from big endian to little endian if necessary
    read((char*)&value, sizeof(long long)) ;
    return ntohl(value) ;
}

/*
 *
 */
p<string> BinaryReader::readString() throw(IOException)
{
    p<string> value ;
    char*     buffer ;
    short     length ;

    // Read length of string
    length = readShort() ;
    buffer = new char[length+1] ;

    // Read string bytes
    read(buffer, length) ;
    *(buffer+length) = '\0' ;

    // Create string class
    value = new string() ;
    value->assign(buffer) ;

    return value ;
}
