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
#ifndef MessageProducer_hpp_
#define MessageProducer_hpp_

#include <string>
#include "IDestination.hpp"
#include "IMessage.hpp"
#include "IMessageProducer.hpp"
#include "Session.hpp"
#include "command/ProducerInfo.hpp"
#include "util/ifr/p"

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
class MessageProducer : public IMessageProducer
{
private:
    p<Session>      session ;
    p<ProducerInfo> producerInfo ;

public:
    MessageProducer(p<Session> session, p<ProducerInfo> producerInfo) ;
    ~MessageProducer() ;

    virtual void send(p<IMessage> message) ;
    virtual void send(p<IDestination> destination, p<IMessage> message) ;
} ;

/* namespace */
    }
  }
}

#endif /*IMessageProducer_hpp_*/