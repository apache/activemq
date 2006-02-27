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
#include "command/ActiveMQBytesMessage.hpp"

using namespace apache::activemq::client::command;

/*
 * 
 */
ActiveMQBytesMessage::ActiveMQBytesMessage()
{
    this->body       = new char[INITIAL_SIZE] ;
    this->bodySize   = INITIAL_SIZE ;
    this->bodyLength = 0 ;
    this->offset     = 0 ;
    this->readMode   = false ;
}

/*
 * 
 */
ActiveMQBytesMessage::~ActiveMQBytesMessage()
{
}

/*
 * 
 */
int ActiveMQBytesMessage::getCommandType()
{
    return ActiveMQBytesMessage::TYPE ;
}

/*
 *
 */
int ActiveMQBytesMessage::getBodyLength()
{
    return bodyLength ;
}

/*
 *
 */
void ActiveMQBytesMessage::reset()
{
    readMode = true ;
    offset   = 0 ;
}

/*
 *
 */
char ActiveMQBytesMessage::readByte() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    // Check for EOF offset
    if( offset > bodySize )
        throw MessageEOFException() ;

    // Read a single byte
    return body[offset++] ;
}

/*
 *
 */
int ActiveMQBytesMessage::readBytes(char* buffer, int length) throw (MessageNotReadableException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    // Check for EOF offset
    if( offset > bodySize )
        return -1 ;

    // Copy bytes to supplied buffer
    for( int i = 0 ; i < length ; i++, offset++ )
    {
        // Check for body EOF
        if( offset > bodySize )
            return i ;

        buffer[i] = body[offset] ;
    }
    return length ;
}

/*
 *
 */
bool ActiveMQBytesMessage::readBoolean() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    bool value ;
    int  result ;

    // Read a boolean
    result = readBytes((char*)&value, sizeof(bool)) ;

    // Check for EOF offset
    if( result == -1 || result < sizeof(bool) )
        throw MessageEOFException() ;

    return value ;
}

/*
 *
 */
double ActiveMQBytesMessage::readDouble() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    double value ;
    int    result ;

    // Read a double
    result = readBytes((char*)&value, sizeof(double)) ;

    // Check for EOF offset
    if( result == -1 || result < sizeof(double) )
        throw MessageEOFException() ;

    // Convert from big endian to little endian if necessary
    return ntohd(value) ;
}

/*
 *
 */
float ActiveMQBytesMessage::readFloat() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    float value ;
    int   result ;

    // Read a float
    result = readBytes((char*)&value, sizeof(float)) ;

    // Check for EOF offset
    if( result == -1 || result < sizeof(float) )
        throw MessageEOFException() ;

    // Convert from big endian to little endian if necessary
    return ntohf(value) ;
}

/*
 *
 */
short ActiveMQBytesMessage::readShort() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    short value ;
    int   result ;

    // Read a short
    result = readBytes((char*)&value, sizeof(short)) ;

    // Check for EOF offset
    if( result == -1 || result < sizeof(short) )
        throw MessageEOFException() ;

    // Convert from big endian to little endian if necessary
    return ntohs(value) ;
}

/*
 *
 */
int ActiveMQBytesMessage::readInt() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    int value ;
    int result ;

    // Read an integer
    result = readBytes((char*)&value, sizeof(int)) ;

    // Check for EOF offset
    if( result == -1 || result < sizeof(int) )
        throw MessageEOFException() ;

    // Convert from big endian to little endian if necessary
    return ntohi(value) ;
}

/*
 *
 */
long long ActiveMQBytesMessage::readLong() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    long long value ;
    int       result ;

    // Read a long long
    result = readBytes((char*)&value, sizeof(long long)) ;

    // Check for EOF offset
    if( result == -1 || result < sizeof(long long) )
        throw MessageEOFException() ;

    // Convert from big endian to little endian if necessary
    return ntohl(value) ;
}

/*
 *
 */
p<string> ActiveMQBytesMessage::readUTF() throw(MessageNotReadableException, MessageEOFException)
{
    // TODO
    return NULL ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeByte(char value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Check for EOF offset
    if( offset > bodySize )
        expandBody() ;

    // Write a single byte
    body[offset++] = value ;

    // Update content length
    bodyLength = offset ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeBytes(char* value, int length) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Copy bytes from supplied buffer
    for( int i = 0 ; i < length ; i++, offset++ )
    {
        // Check for EOF offset
        if( offset > bodySize )
            expandBody() ;

        body[offset] = value[i] ;
    }
    // Update content length
    bodyLength = offset ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeBoolean(bool value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write boolean
    writeBytes((char*)&value, sizeof(bool)) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeDouble(double v) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write double, convert from little endian to big endian if necessary
    double value = htond(v) ;
    writeBytes((char*)&value, sizeof(double)) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeFloat(float v) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write float, convert from little endian to big endian if necessary
    float value = htonf(v) ;
    writeBytes((char*)&value, sizeof(float)) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeInt(int v) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write integer, convert from little endian to big endian if necessary
    int value = htoni(v) ;
    writeBytes((char*)&value, sizeof(int)) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeLong(long long v) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write long, convert from little endian to big endian if necessary
    long long value = htonl(v) ;
    writeBytes((char*)&value, sizeof(long long)) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeShort(short v) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write short, convert from little endian to big endian if necessary
    short value = htons(v) ;
    writeBytes((char*)&value, sizeof(short)) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeUTF(p<string> value) throw (MessageNotWritableException)
{
    // TODO
}

/*
 *
 */
void ActiveMQBytesMessage::expandBody()
{
    char* newBody ;
    int   newSize = bodySize + ActiveMQBytesMessage::EXPAND_SIZE ;

    // Create new body and copy current contents
    newBody = new char[ newSize ] ;
    memcpy(newBody, body, bodySize) ;

    // Clean up and use new content body
    delete [] body ;
    body     = newBody ;
    bodySize = newSize ;
}
