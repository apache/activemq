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
#ifndef ActiveMQ_ActiveMQTempQueue_hpp_
#define ActiveMQ_ActiveMQTempQueue_hpp_

#include "cms/ITemporaryQueue.hpp"
#include "activemq/command/ActiveMQTempDestination.hpp"

namespace apache
{
  namespace activemq
  {
    namespace command
    {
        using namespace apache::cms;

/*
 * 
 */
class ActiveMQTempQueue : public ActiveMQTempDestination, public ITemporaryQueue
{
public:
    const static unsigned char TYPE = 102 ;

public:
    ActiveMQTempQueue() ;
    ActiveMQTempQueue(const char* name) ;
    virtual ~ActiveMQTempQueue() ;

    virtual unsigned char getDataStructureType() ;
    virtual p<string> getQueueName() ;
    virtual int getDestinationType() ;
    virtual p<ActiveMQDestination> createDestination(const char* name) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ActiveMQTempQueue_hpp_*/
