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
#include "ppr/io/DataOutputStream.hpp"

using namespace apache::ppr::io;

/*
 *
 */
DataOutputStream::DataOutputStream(p<IOutputStream> ostream)
{
    this->ostream = ostream ;
    this->encoder = CharsetEncoderRegistry::getEncoder() ;
}

/*
 *
 */
DataOutputStream::DataOutputStream(p<IOutputStream> ostream, const char* encname)
{
    this->ostream = ostream ;
    this->encoder = CharsetEncoderRegistry::getEncoder(encname) ;
}

/*
 *
 */
DataOutputStream::~DataOutputStream()
{
    // no-op
}

/*
 *
 */
void DataOutputStream::close() throw(IOException)
{
    // Cascade close request to underlying stream
    if( ostream != NULL )
    {
        ostream->close() ;
        ostream = NULL ;
    }
}

/*
 *
 */
void DataOutputStream::flush() throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Flush stream
    ostream->flush() ;
}

/*
 *
 */
int DataOutputStream::write(const char* buffer, int offset, int length) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write buffer
    return ostream->write(buffer, offset, length) ;
}

/*
 *
 */
void DataOutputStream::writeByte(char value) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write a single byte
    ostream->write((char*)&value, 0, sizeof(char)) ;
}

/*
 *
 */
void DataOutputStream::writeBoolean(bool value) throw(IOException)
{
    // Write a boolean
    ( value ) ? writeByte(0x01) : writeByte(0x00) ;
}

/*
 *
 */
void DataOutputStream::writeDouble(double v) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write a double, convert from little endian to big endian if necessary
    double value = htond(v) ;
    ostream->write((char*)&value, 0, sizeof(double)) ;
}

/*
 *
 */
void DataOutputStream::writeFloat(float v) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write a float, convert from little endian to big endian if necessary
    float value = htonf(v) ;
    ostream->write((char*)&value, 0, sizeof(float)) ;
}

/*
 *
 */
void DataOutputStream::writeShort(short v) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write a short, convert from little endian to big endian if necessary
    short value = htons(v) ;
    ostream->write((char*)&value, 0, sizeof(short)) ;
}

/*
 *
 */
void DataOutputStream::writeInt(int v) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write an int, convert from little endian to big endian if necessary
    int value = htoni(v) ;
    ostream->write((char*)&value, 0, sizeof(int)) ;
}

/*
 *
 */
void DataOutputStream::writeLong(long long v) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Write a long long, convert from little endian to big endian if necessary
    long long value = htonll(v) ;
    ostream->write((char*)&value, 0, sizeof(long long)) ;
}

/*
 *
 */
int DataOutputStream::writeString(p<string> value) throw(IOException)
{
    // Check if underlying stream has been closed
    checkClosed() ;

    // Assert argument
    if( value == NULL )
        return 0 ;

    p<string> data ;
    int       length = (int)value->length() ;

    // Encode string if an charset encoder has been configured
    if( encoder != NULL )
    {
        try {
            data = encoder->encode(value, &length) ;
        }
        catch( CharsetEncodingException &cee ) {
            throw IOException( cee.what() ) ;
        }
    }
    else
        data = value ;

    // Assert string length
    if( length > USHRT_MAX )
        throw IOException("String length exceeds maximum length") ;

    // Write length of string
    writeShort( (short)length ) ;

    // Write string contents
    ostream->write((char*)data->c_str(), 0, length) ;

    return length ;
}

/*
 * Check if stream has been closed.
 */
void DataOutputStream::checkClosed() throw(IOException)
{
    if( ostream == NULL )
        throw IOException("Output stream closed") ;
}
