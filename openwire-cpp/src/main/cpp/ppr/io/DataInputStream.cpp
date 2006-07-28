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
#include "ppr/io/DataInputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
DataInputStream::DataInputStream()
{
    // no-op
}

/*
 *
 */
DataInputStream::~DataInputStream()
{
    // no-op
}

/*
 *
 */
char DataInputStream::readByte() throw(IOException)
{
    char value ;

    // Read a single byte
    read(&value, 0, sizeof(char)) ;
    return value ;
}

/*
 *
 */
bool DataInputStream::readBoolean() throw(IOException)
{
    // Read a boolean
    return ( readByte() ) ? true : false ;
}

/*
 *
 */
double DataInputStream::readDouble() throw(IOException)
{
    double value ;

    // Read a double and convert from big endian to little endian if necessary
    read((char*)&value, 0, sizeof(double) ) ;
    return ntohd(value) ;
}

/*
 *
 */
float DataInputStream::readFloat() throw(IOException)
{
    float value ;

    // Read a float and convert from big endian to little endian if necessary
    read((char*)&value, 0, sizeof(float)) ;
    return ntohf(value) ;
}

/*
 *
 */
short DataInputStream::readShort() throw(IOException)
{
    short value ;

    // Read a short and convert from big endian to little endian if necessary
    read((char*)&value, 0, sizeof(short)) ;
    return ntohs(value) ;
}

/*
 *
 */
int DataInputStream::readInt() throw(IOException)
{
    int value ;

    // Read an int and convert from big endian to little endian if necessary
    read((char*)&value, 0, sizeof(int)) ;
    return ntohi(value) ;
}

/*
 *
 */
long long DataInputStream::readLong() throw(IOException)
{
    long long value ;

    // Read a long long and convert from big endian to little endian if necessary
    read((char*)&value, 0, sizeof(long long)) ;
    return ntohll(value) ;
}

/*
 *
 */
p<string> DataInputStream::readString() throw(IOException)
{
    p<string> value ;
    char*     buffer ;
    short     length ;

    // Read length of string
    length = readShort() ;
    if (length < 0) {
        throw IOException ("Negative length of string");
    } else if (length > 0) {
        buffer = new char[length+1] ;

        // Read string bytes
        read(buffer, 0, length) ;
        *(buffer+length) = '\0' ;

        // Create string class
        value = new string() ;
        value->assign(buffer) ;

        return value ;
    } else {
        return new string ("");
    }
}
