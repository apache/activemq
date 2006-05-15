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
#include "activemq/command/ActiveMQBytesMessage.hpp"

using namespace apache::activemq::command;


/*
 * 
 */
ActiveMQBytesMessage::ActiveMQBytesMessage()
{
    this->bis      = NULL ;
    this->bos      = new ByteArrayOutputStream() ;
    this->dis      = NULL ;
    this->dos      = new DataOutputStream( bos ) ;
    this->readMode = false ;
}

/*
 * 
 */
ActiveMQBytesMessage::ActiveMQBytesMessage(char* body, int size)
{
    // Convert body to SP array
    array<char> buffer = array<char> (size) ;
    memcpy(buffer.c_array(), body, size);

    this->bis      = NULL ;
    this->bos      = new ByteArrayOutputStream(buffer) ;
    this->dis      = NULL ;
    this->dos      = new DataOutputStream( bos ) ;
    this->readMode = false ;
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
unsigned char ActiveMQBytesMessage::getDataStructureType()
{
    return ActiveMQBytesMessage::TYPE ;
}

/*
 *
 */
void ActiveMQBytesMessage::reset()
{
    if( readMode )
    {
        this->bos      = new ByteArrayOutputStream( bis->toArray() ) ;
        this->bis      = NULL ;
        this->dos      = new DataOutputStream( bos ) ;
        this->dis      = NULL ;
        this->readMode = false ;
    }
    else
    {
        this->bis      = new ByteArrayInputStream( bos->toArray() ) ;
        this->bos      = NULL ;
        this->dis      = new DataInputStream( bis ) ;
        this->dos      = NULL ;
        this->readMode = true ;
    }
}

/*
 *
 */
char ActiveMQBytesMessage::readByte() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a single byte
        return dis->readByte() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
int ActiveMQBytesMessage::readBytes(char* buffer, int offset, int length) throw (MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read some bytes
        return dis->read(buffer, offset, length) ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
bool ActiveMQBytesMessage::readBoolean() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a boolean
        return dis->readBoolean() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
double ActiveMQBytesMessage::readDouble() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a double
        return dis->readDouble() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
float ActiveMQBytesMessage::readFloat() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a float
        return dis->readFloat() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
short ActiveMQBytesMessage::readShort() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a short
        return dis->readShort() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
int ActiveMQBytesMessage::readInt() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read an integer
        return dis->readInt() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
long long ActiveMQBytesMessage::readLong() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a long long
        return dis->readLong() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
p<string> ActiveMQBytesMessage::readString() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a string
        return dis->readString() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 * 
 */
void ActiveMQBytesMessage::writeByte(char value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a single byte
    dos->writeByte(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeBytes(char* value, int offset, int length) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write some bytes
    dos->write(value, offset, length) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeBoolean(bool value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a boolean
    dos->writeBoolean(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeDouble(double value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a double
    dos->writeDouble(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeFloat(float value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a float
    dos->writeFloat(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeInt(int value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write an integer
    dos->writeInt(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeLong(long long value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a long long
    dos->writeLong(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeShort(short value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a short
    dos->writeShort(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeString(const char* value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a string
    p<string> v = new string(value) ;
    dos->writeString(v) ;
}

/*
 *
 */
int ActiveMQBytesMessage::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw (IOException)
{
    int size = 0 ;

    // Copy body to message content container
    if( mode == IMarshaller::MARSHAL_SIZE )
        this->content = ( readMode) ? bis->toArray() : bos->toArray() ;

//    size += (int)this->content.size() ;
    size += ActiveMQMessage::marshal(marshaller, mode, ostream) ;

    // Note! Message content marshalling is done in super class

    return size ;
}

/*
 *
 */
void ActiveMQBytesMessage::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw (IOException)
{
    // Note! Message content unmarshalling is done in super class
    ActiveMQMessage::unmarshal(marshaller, mode, istream) ;

    // Copy body to message content holder
    if( mode == IMarshaller::MARSHAL_READ )
    {
        bis      = new ByteArrayInputStream( this->content ) ;
        bos      = NULL ;
        dis      = new DataInputStream( bis ) ;
        dos      = NULL ;
        readMode = true ;
    }
}
