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
#ifndef ActiveMQ_ActiveMQMapMessage_hpp_
#define ActiveMQ_ActiveMQMapMessage_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <map>
#include <string>
#include "cms/IMapMessage.hpp"
#include "cms/MessageFormatException.hpp"
#include "activemq/command/ActiveMQMessage.hpp"
#include "ppr/IllegalArgumentException.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
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
      using namespace std;
      using namespace apache::cms;
      using namespace apache::ppr;
      using namespace apache::ppr::io;
      using namespace apache::ppr::util;

/*
 * 
 */
class ActiveMQMapMessage : public ActiveMQMessage, public IMapMessage
{
private:
    p<string>      text ;
    p<PropertyMap> contentMap ;

public:
    const static unsigned char TYPE = 25 ;

public:
    ActiveMQMapMessage() ;
    virtual ~ActiveMQMapMessage() ;

    virtual unsigned char getDataStructureType() ;

    virtual p<PropertyMap> getBody() ;
    virtual bool getBoolean(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setBoolean(const char* name, bool value) throw (IllegalArgumentException) ;
    virtual char getByte(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setByte(const char* name, char value) throw (IllegalArgumentException) ;
    virtual array<char> getBytes(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setBytes(const char* name, array<char> value) throw (IllegalArgumentException) ;
    virtual double getDouble(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setDouble(const char* name, double value) throw (IllegalArgumentException) ;
    virtual float getFloat(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setFloat(const char* name, float value) throw (IllegalArgumentException) ;
    virtual int getInt(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setInt(const char* name, int value) throw (IllegalArgumentException) ;
    virtual long long getLong(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setLong(const char* name, long long value) throw (IllegalArgumentException) ;
    virtual short getShort(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setShort(const char* name, short value) throw (IllegalArgumentException) ;
    virtual p<string> getString(const char* name) throw (MessageFormatException, IllegalArgumentException) ;
    virtual void setString(const char* name, const char* value) throw (IllegalArgumentException) ;
    virtual array<string> getMapNames() ;
    virtual bool itemExists(const char* name) ;

    virtual int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw (IOException) ;
    virtual void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw (IOException) ;

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
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ActiveMQMapMessage_hpp_*/
