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
#ifndef ActiveMQ_ActiveMQBytesMessage_hpp_
#define ActiveMQ_ActiveMQBytesMessage_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <map>
#include <string>
#include "cms/IBytesMessage.hpp"
#include "cms/MessageEOFException.hpp"
#include "cms/MessageNotWritableException.hpp"
#include "activemq/command/ActiveMQMessage.hpp"
#include "ppr/io/ByteArrayInputStream.hpp"
#include "ppr/io/ByteArrayOutputStream.hpp"
#include "ppr/io/EOFException.hpp"
#include "ppr/util/Endian.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace command
    {
      using namespace ifr;
      using namespace apache::cms;
      using namespace apache::ppr::io;
      using namespace apache::ppr::util;

/*
 * 
 */
class ActiveMQBytesMessage : public ActiveMQMessage , public IBytesMessage
{
private:
    p<ByteArrayInputStream>  in ;
    p<ByteArrayOutputStream> out ;
    bool  readMode ;

    const static int INITIAL_SIZE = 256 ;
    const static int EXPAND_SIZE  = 128 ;

public:
    const static unsigned char TYPE = 24 ;

public:
    ActiveMQBytesMessage() ;
    ActiveMQBytesMessage(char* body, int size) ;
    virtual ~ActiveMQBytesMessage() ;

    virtual unsigned char getDataStructureType() ;

    virtual void reset() ;
    virtual char readByte() throw (MessageNotReadableException, MessageEOFException) ;
    virtual int readBytes(char* buffer, int index, int length) throw (MessageNotReadableException, MessageEOFException) ;
    virtual bool readBoolean() throw (MessageNotReadableException, MessageEOFException) ;
    virtual double readDouble() throw (MessageNotReadableException, MessageEOFException) ;
    virtual float readFloat() throw (MessageNotReadableException, MessageEOFException) ;
    virtual int readInt() throw (MessageNotReadableException, MessageEOFException) ;
    virtual long long readLong() throw (MessageNotReadableException, MessageEOFException) ;
    virtual short readShort() throw (MessageNotReadableException, MessageEOFException) ;
    virtual p<string> readUTF() throw (MessageNotReadableException, MessageEOFException) ;
    virtual void writeBoolean(bool value) throw (MessageNotWritableException) ;
    virtual void writeByte(char value) throw (MessageNotWritableException) ;
    virtual void writeBytes(char* value, int index, int length) throw (MessageNotWritableException) ;
    virtual void writeDouble(double value) throw (MessageNotWritableException) ;
    virtual void writeFloat(float value) throw (MessageNotWritableException) ;
    virtual void writeInt(int value) throw (MessageNotWritableException) ;
    virtual void writeLong(long long value) throw (MessageNotWritableException) ;
    virtual void writeShort(short value) throw (MessageNotWritableException) ;
    virtual void writeUTF(const char* value) throw (MessageNotWritableException) ;

    virtual int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException) ;
    virtual void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException) ;

    //
    // The methods below are needed to resolve the multiple
    // inheritance of IMessage.
    virtual void acknowledge() {
        ActiveMQMessage::acknowledge() ;
    } ;
    virtual p<PropertyMap> getProperties() {
        return ActiveMQMessage::getProperties() ;
    } ;
    virtual p<string> getJMSCorrelationID() {
        return ActiveMQMessage::getJMSCorrelationID() ;
    } ;
    virtual void setJMSCorrelationID(const char* correlationId) {
        return ActiveMQMessage::setJMSCorrelationID(correlationId) ;
    } ;
    virtual p<IDestination> getJMSDestination() {
        return ActiveMQMessage::getJMSDestination() ;
    } ;
    virtual long long getJMSExpiration() {
        return ActiveMQMessage::getJMSExpiration() ;
    } ;
    virtual void setJMSExpiration(long long time) {
        return ActiveMQMessage::setJMSExpiration(time) ;
    } ;
    virtual p<string> getJMSMessageID() {
        return ActiveMQMessage::getJMSMessageID() ;
    } ;
    virtual bool getJMSPersistent() {
        return ActiveMQMessage::getJMSPersistent() ;
    } ;
    virtual void setJMSPersistent(bool persistent) {
        return ActiveMQMessage::setJMSPersistent(persistent) ;
    } ;
    virtual unsigned char getJMSPriority() {
        return ActiveMQMessage::getJMSPriority() ;
    } ;
    virtual void setJMSPriority(unsigned char priority) {
        return ActiveMQMessage::setJMSPriority(priority) ;
    } ;
    virtual bool getJMSRedelivered() {
        return ActiveMQMessage::getJMSRedelivered() ;
    } ;
    virtual p<IDestination> getJMSReplyTo() {
        return ActiveMQMessage::getJMSReplyTo() ;
    } ;
    virtual void setJMSReplyTo(p<IDestination> destination) {
        return ActiveMQMessage::setJMSReplyTo(destination) ;
    } ;
    virtual long long getJMSTimestamp() {
        return ActiveMQMessage::getJMSTimestamp() ;
    } ;
    virtual p<string> getJMSType() {
        return ActiveMQMessage::getJMSType() ;
    } ;
    virtual void setJMSType(const char* type) {
        return ActiveMQMessage::setJMSType(type) ;
    } ;
    virtual int getJMSXDeliveryCount() {
        return ActiveMQMessage::getJMSXDeliveryCount() ;
    } ;
    virtual p<string> getJMSXGroupID() {
        return ActiveMQMessage::getJMSXGroupID() ;
    } ;
    virtual void setJMSXGroupID(const char* groupId) {
        return ActiveMQMessage::setJMSXGroupID(groupId) ;
    } ;
    virtual int getJMSXGroupSeq() {
        return ActiveMQMessage::getJMSXGroupSeq() ;
    } ;
    virtual void setJMSXGroupSeq(int sequence) {
        return ActiveMQMessage::setJMSXGroupSeq(sequence) ;
    } ;
    virtual p<string> getJMSXProducerTxID() {
        return ActiveMQMessage::getJMSXProducerTxID() ;
    } ;

//private:
  //  void expandBody() ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ActiveMQBytesMessage_hpp_*/
