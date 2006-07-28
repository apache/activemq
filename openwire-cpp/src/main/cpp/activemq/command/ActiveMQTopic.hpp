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
#ifndef ActiveMQ_ActiveMQTopic_hpp_
#define ActiveMQ_ActiveMQTopic_hpp_

#include "cms/ITopic.hpp"
#include "activemq/command/ActiveMQDestination.hpp"

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
class ActiveMQTopic : public ActiveMQDestination, public ITopic
{
public:
    const static unsigned char TYPE = 101 ;

public:
    ActiveMQTopic() ;
    ActiveMQTopic(const char* name) ;
    virtual ~ActiveMQTopic() ;

    virtual unsigned char getDataStructureType() ;
    virtual p<string> getTopicName() ;
    virtual int getDestinationType() ;
    virtual p<ActiveMQDestination> createDestination(const char* name) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ActiveMQTopic_hpp_*/
