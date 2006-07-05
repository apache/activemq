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
#ifndef Cms_IMessageProducer_hpp_
#define Cms_IMessageProducer_hpp_

#include <string>
#include "cms/IDestination.hpp"
#include "cms/IMessage.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace cms
  {
    using namespace ifr;

/*
 * An object capable of sending messages to some destination.
 */
struct IMessageProducer : Interface
{
    // Sends the message to the default destination for this producer.
    virtual void send(p<IMessage> message) = 0 ;

    // Sends the message to the given destination.
    virtual void send(p<IDestination> destination, p<IMessage> message) = 0 ;

    virtual void close() = 0 ;

	virtual bool getPersistent() = 0 ;
	virtual void setPersistent(bool persistent) = 0 ;
    virtual long long getTimeToLive() = 0 ;
    virtual void getTimeToLive(long long ttl) = 0 ;
    virtual int getPriority() = 0 ;
    virtual void getPriority(int priority) = 0 ;
    virtual bool getDisableMessageID() = 0 ;
    virtual void getDisableMessageID(bool disable) = 0 ;
    virtual bool getDisableMessageTimestamp() = 0 ;
    virtual void getDisableMessageTimestamp(bool disable) = 0 ;
} ;

/* namespace */
  }
}

#endif /*Cms_IMessageProducer_hpp_*/
