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
#ifndef MessageConsumer_hpp_
#define MessageConsume_hpp_

#include <string>
#include "IDestination.hpp"
#include "IMessage.hpp"
#include "IMessageConsumer.hpp"
#include "Session.hpp"
#include "OpenWireException.hpp"
#include "ConnectionClosedException.hpp"
#include "command/ConsumerInfo.hpp"
#include "util/ifr/p"

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace ifr;

/*
 * 
 */
class MessageConsumer : public IMessageConsumer
{
private:
    p<Session>      session ;
    p<ConsumerInfo> consumerInfo ;
    bool            closed ;

public:
    MessageConsumer(p<Session> session, p<ConsumerInfo> consumerInfo) ;
    ~MessageConsumer() ;

    /* TODO
    virtual void setMessageListener(p<IMessageListener> listener) ;
    virtual p<IMessageListener> getMessageListener() ;
    */

    virtual p<IMessage> receive() ;
    virtual p<IMessage> receiveNoWait() ;

protected:
    void checkClosed() throw(OpenWireException) ;
} ;

/* namespace */
    }
  }
}

#endif /*IMessageConsumer_hpp_*/