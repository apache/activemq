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
#ifndef ISession_hpp_
#define ISession_hpp_

#include <string>
#include "IDestination.hpp"
#include "IMessageProducer.hpp"
#include "IMessageConsumer.hpp"
#include "IQueue.hpp"
#include "ITopic.hpp"
#include "ITextMessage.hpp"
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
struct ISession
{
    virtual p<IMessageProducer> createProducer() = 0 ;
    virtual p<IMessageProducer> createProducer(p<IDestination> destination) = 0 ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination) = 0 ;
    virtual p<IMessageConsumer> createConsumer(p<IDestination> destination, const char* selector) = 0 ;
    virtual p<IQueue> getQueue(const char* name) = 0 ;
    virtual p<ITopic> getTopic(const char* name) = 0 ;
    virtual p<IMessage> createMessage() = 0 ;
    virtual p<ITextMessage> createTextMessage() = 0 ;
    virtual p<ITextMessage> createTextMessage(const char* text) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*ISession_hpp_*/