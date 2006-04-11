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
#ifndef ActiveMQTempTopic_hpp_
#define ActiveMQTempTopic_hpp_

#include "ITemporaryTopic.hpp"
#include "command/ActiveMQDestination.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {

/*
 * 
 */
class ActiveMQTempTopic : public ActiveMQDestination, ITemporaryTopic
{
public:
    const static char TYPE = 103 ;

public:
    ActiveMQTempTopic() ;
    ActiveMQTempTopic(const char* name) ;
    virtual ~ActiveMQTempTopic() ;

    virtual const char* getTopicName() ;
    virtual int getDestinationType() ;
    virtual p<ActiveMQDestination> createDestination(const char* name) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*ActiveMQTempTopic_hpp_*/
