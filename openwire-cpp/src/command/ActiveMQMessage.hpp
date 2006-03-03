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
#ifndef ActiveMQMessage_hpp_
#define ActiveMQMessage_hpp_

#include <string>
#include "IDestination.hpp"
#include "IMessage.hpp"
#include "command/Message.hpp"
#include "util/ifr/p.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr;
        using namespace apache::activemq::client;

/*
 * Dummy, should be auto-generated.
 */
class ActiveMQMessage : public Message, IMessage
{
private:
    p<IDestination> destination ;

public:
    const static int TYPE = 23 ;

public:
    ActiveMQMessage() ;
    ActiveMQMessage(const char* name) ;
    virtual ~ActiveMQMessage() ;

    virtual p<IDestination> getFromDestination() ;
    virtual void setFromDestination(p<IDestination> destination) ;

    static p<ActiveMQMessage> transform(p<IMessage> message) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*ActiveMQMessage_hpp_*/