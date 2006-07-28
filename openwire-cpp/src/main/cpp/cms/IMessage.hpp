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
#ifndef Cms_IMessage_hpp_
#define Cms_IMessage_hpp_

#include <string>
#include "cms/IDestination.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace cms
  {
    using namespace apache::ppr::util;
    using namespace ifr;
    using namespace std;

/*
 * Represents a message either to be sent to a message broker
 * or received from a message broker.
 */
struct IMessage : Interface
{
    // If using client acknowledgement mode on the session then
    // this method will acknowledge that the message has been
    // processed correctly.
    virtual void acknowledge() = 0 ;

    // Provides access to the message properties (headers).
    virtual p<PropertyMap> getProperties() = 0 ;

    // The correlation ID used to correlate messages from
    // conversations or long running business processes.
    virtual p<string> getJMSCorrelationID() = 0 ;
    virtual void setJMSCorrelationID(const char* correlationId) = 0 ;

    // The destination of the message.
    virtual p<IDestination> getJMSDestination() = 0 ;

    // The time in milliseconds that this message should expire.
    virtual long long getJMSExpiration() = 0 ;
    virtual void setJMSExpiration(long long time) = 0 ;

    // The message ID which is set by the provider.
    virtual p<string> getJMSMessageID() = 0 ;

    // Whether or not this message is persistent.
    virtual bool getJMSPersistent() = 0 ;
    virtual void setJMSPersistent(bool persistent) = 0 ;

    // The priority on this message.
    virtual unsigned char getJMSPriority() = 0 ;
    virtual void setJMSPriority(unsigned char priority) = 0 ;

    // Returns true if this message has been redelivered to this
    // or another consumer before being acknowledged successfully.
    virtual bool getJMSRedelivered() = 0 ;

    // The destination that the consumer of this message should
    // send replies to.
    virtual p<IDestination> getJMSReplyTo() = 0 ;
    virtual void setJMSReplyTo(p<IDestination> destination) = 0 ;

    // The timestamp the broker added to the message.
    virtual long long getJMSTimestamp() = 0 ;

    // The type name of this message.
    virtual p<string> getJMSType() = 0 ;
    virtual void setJMSType(const char* type) = 0 ;

    //
    // JMS Extension Headers

    // Returns the number of times this message has been redelivered
    // to other consumers without being acknowledged successfully.
    virtual int getJMSXDeliveryCount() = 0 ;

    // The message group ID is used to group messages together to the
    // same consumer for the same group ID value.
    virtual p<string> getJMSXGroupID() = 0 ;
    virtual void setJMSXGroupID(const char* groupId) = 0 ;

    // The message group sequence counter to indicate the position
    // in a group.
    virtual int getJMSXGroupSeq() = 0 ;
    virtual void setJMSXGroupSeq(int sequence) = 0 ;

    // Returns the ID of the producers transaction.
    virtual p<string> getJMSXProducerTxID() = 0 ;
} ;

/* namespace */
  }
}

#endif /*Cms_IMessage_hpp_*/
