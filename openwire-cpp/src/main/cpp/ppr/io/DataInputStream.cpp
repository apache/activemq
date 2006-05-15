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
#include "ppr/io/DataInputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
DataInputStream::DataInputStream(p<IInputStream> istream)
{
    this->istream = istream ;
    this->encoder = CharsetEncoderRegistry::getEncoder() ;
}

/*
 *
 */
DataInputStream::DataInputStream(p<IInputStream> istream, const char* encname)
{
    this->istream = istream ;
    this->encoder = CharsetEncoderRegistry::getEncoder(encname) ;
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
void DataInputStream::close() throw(IOException)
{
    // Cascade close request to underlying stream
    if( istream != NULL )
    {
        istream->close() ;
        istream = NULL ;
    }
}

/*
 *
 */
int DataInputStream::read(char* buffer, int offset, int length) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Read buffer
    return istream->read(buffer, offset, length) ;
}

/*
 *
 */
char DataInputStream::readByte() throw(IOException)
{
    char value ;

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read a single byte
    istream->read(&value, 0, sizeof(char)) ;
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

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read a double and convert from big endian to little endian if necessary
    istream->read((char*)&value, 0, sizeof(double) ) ;
    return ntohd(value) ;
}

/*
 *
 */
float DataInputStream::readFloat() throw(IOException)
{
    float value ;

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read a float and convert from big endian to little endian if necessary
    istream->read((char*)&value, 0, sizeof(float)) ;
    return ntohf(value) ;
}

/*
 *
 */
short DataInputStream::readShort() throw(IOException)
{
    short value ;

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read a short and convert from big endian to little endian if necessary
    istream->read((char*)&value, 0, sizeof(short)) ;
    return ntohs(value) ;
}

/*
 *
 */
int DataInputStream::readInt() throw(IOException)
{
    int value ;

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read an int and convert from big endian to little endian if necessary
    istream->read((char*)&value, 0, sizeof(int)) ;
    return ntohi(value) ;
}

/*
 *
 */
long long DataInputStream::readLong() throw(IOException)
{
    long long value ;

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read a long long and convert from big endian to little endian if necessary
    istream->read((char*)&value, 0, sizeof(long long)) ;
    return ntohll(value) ;
}

/*
 *
 */
p<string> DataInputStream::readString() throw(IOException)
{
    p<string> value ;
    short     length ;

    // Check if underlying stream has been closed
    checkClosed() ;

    // Read length of string
    length = readShort() ;
    if (length < 0)
        throw IOException ("Negative length of string") ;
    else if (length > 0)
    {
        array<char> buffer = array<char> (length+1) ;

        // Read string bytes
        istream->read(&buffer[0], 0, length) ;
        buffer[length] = '\0' ;

        // Create string
        value = new string() ;
        value->assign( buffer.c_array() ) ;

        // Decode string if charset encoder has been configured
        if( encoder != NULL )
            value = encoder->decode(value) ;
    }
    else   // ...empty string
        value = new string("") ;

    return value ;
}

/*
 * Check if stream has been closed.
 */
void DataInputStream::checkClosed() throw(IOException)
{
    if( istream == NULL )
        throw IOException("Input stream closed") ;
}
