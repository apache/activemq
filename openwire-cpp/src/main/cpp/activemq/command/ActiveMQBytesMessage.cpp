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
    this->in       = NULL ;
    this->out      = new ByteArrayOutputStream() ;
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

    this->in       = NULL ;
    this->out      = new ByteArrayOutputStream(buffer) ;
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
    if( !readMode )
    {
        this->in       = new ByteArrayInputStream( out->toArray() ) ;
        this->out      = NULL ;
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
        return readByte() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
int ActiveMQBytesMessage::readBytes(char* buffer, int index, int length) throw (MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read some bytes
        return in->read(buffer, index, length) ;
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
        return in->readBoolean() ;
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
        return in->readDouble() ;
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
        return in->readFloat() ;
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
        return in->readShort() ;
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
        return in->readInt() ;
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
        return in->readLong() ;
    }
    catch( EOFException eof )
    {
        throw MessageEOFException() ;
    }
}

/*
 *
 */
p<string> ActiveMQBytesMessage::readUTF() throw(MessageNotReadableException, MessageEOFException)
{
    // Assert read mode
    if( !readMode )
        throw MessageNotReadableException() ;

    try
    {
        // Read a string
        return in->readString() ;
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
    out->writeByte(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeBytes(char* value, int index, int length) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write some bytes
    out->write(value, index, length) ;
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
    out->writeBoolean(value) ;
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
    out->writeDouble(value) ;
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
    out->writeFloat(value) ;
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
    out->writeInt(value) ;
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
    out->writeLong(value) ;
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
    out->writeShort(value) ;
}

/*
 * 
 */
void ActiveMQBytesMessage::writeUTF(const char* value) throw (MessageNotWritableException)
{
    // Assert write mode
    if( readMode )
        throw MessageNotWritableException() ;

    // Write a string
    p<string> v = new string(value) ;
    out->writeString(v) ;
}

/*
 *
 */
int ActiveMQBytesMessage::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException)
{
    int size = 0 ;

    // Copy body to message content container
    if( mode == IMarshaller::MARSHAL_SIZE )
        this->content = ( readMode) ? in->toArray() : out->toArray() ;

//    size += (int)this->content.size() ;
    size += ActiveMQMessage::marshal(marshaller, mode, writer) ;

    // Note! Message content marshalling is done in super class

    return size ;
}

/*
 *
 */
void ActiveMQBytesMessage::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException)
{
    // Note! Message content unmarshalling is done in super class
    ActiveMQMessage::unmarshal(marshaller, mode, reader) ;

    // Copy body to message content holder
    if( mode == IMarshaller::MARSHAL_READ )
    {
        in       = new ByteArrayInputStream( this->content ) ;
        out      = NULL ;
        readMode = true ;
    }
}
