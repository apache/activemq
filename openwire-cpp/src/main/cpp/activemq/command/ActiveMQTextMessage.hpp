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
#ifndef ActiveMQ_ActiveMQTextMessage_hpp_
#define ActiveMQ_ActiveMQTextMessage_hpp_

#include <string>
#include "cms/ITextMessage.hpp"
#include "activemq/command/ActiveMQMessage.hpp"
#include "ppr/io/ByteArrayOutputStream.hpp"
#include "ppr/io/DataOutputStream.hpp"
#include "ppr/io/encoding/ICharsetEncoder.hpp"
#include "ppr/io/encoding/CharsetEncoderRegistry.hpp"
#include "ppr/util/MapItemHolder.hpp"
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
      using namespace apache::ppr::io::encoding;
      using namespace apache::ppr::util;

/*
 * 
 */
class ActiveMQTextMessage : public ActiveMQMessage, public ITextMessage
{
private:
    p<ICharsetEncoder> encoder ;

public:
    const static unsigned char TYPE = 28 ;

public:
    ActiveMQTextMessage() ;
    ActiveMQTextMessage(const char* text) ;
    ActiveMQTextMessage(const char* text, const char* encname) ;
    virtual ~ActiveMQTextMessage() ;

    virtual unsigned char getDataStructureType() ;
    virtual p<string> getText() ;
    virtual void setText(const char* text) ;

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

#endif /*ActiveMQ_ActiveMQTextMessage_hpp_*/
