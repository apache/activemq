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
#ifndef ActiveMQ_ActiveMQMessage_hpp_
#define ActiveMQ_ActiveMQMessage_hpp_

#include <string>
#include "cms/IDestination.hpp"
#include "cms/IMessage.hpp"
#include "activemq/IAcknowledger.hpp"
#include "activemq/command/Message.hpp"
#include "activemq/command/LocalTransactionId.hpp"
#include "activemq/command/XATransactionId.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/util/Hex.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace command
    {
      using namespace ifr;
      using namespace apache::activemq;
      using namespace apache::cms;
      using namespace apache::ppr::io;
      using namespace apache::ppr::util;

/*
 * 
 */
class ActiveMQMessage : public Message, public IMessage
{
private:
    p<IAcknowledger> acknowledger ;
    p<PropertyMap>   properties ;

public:
    const static unsigned char TYPE = 23 ;

public:
    // Attributes
    virtual unsigned char getDataStructureType() ;
    virtual p<IDestination> getFromDestination() ;
    virtual void setFromDestination(p<IDestination> destination) ;
    virtual void setAcknowledger(p<IAcknowledger> acknowledger) ;
    virtual p<PropertyMap> getProperties() ;
    virtual p<string> getJMSCorrelationID() ;
    virtual void setJMSCorrelationID(const char* correlationId) ;
    virtual p<IDestination> getJMSDestination() ;
    virtual long long getJMSExpiration() ;
    virtual void setJMSExpiration(long long time) ;
    virtual p<string> getJMSMessageID() ;
    virtual bool getJMSPersistent() ;
    virtual void setJMSPersistent(bool persistent) ;
    virtual unsigned char getJMSPriority() ;
    virtual void setJMSPriority(unsigned char priority) ;
    virtual bool getJMSRedelivered() ;
    virtual p<IDestination> getJMSReplyTo() ;
    virtual void setJMSReplyTo(p<IDestination> destination) ;
    virtual long long getJMSTimestamp() ;
    virtual p<string> getJMSType() ;
    virtual void setJMSType(const char* type) ;
    virtual int getJMSXDeliveryCount() ;
    virtual p<string> getJMSXGroupID() ;
    virtual void setJMSXGroupID(const char* groupId) ;
    virtual int getJMSXGroupSeq() ;
    virtual void setJMSXGroupSeq(int sequence) ;
    virtual p<string> getJMSXProducerTxID() ;

    // Operations
    virtual void acknowledge() ;

protected:
    // Implementation
    int marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> ostream) throw(IOException) ;
    void unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> istream) throw(IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ActiveMQMessage_hpp_*/
