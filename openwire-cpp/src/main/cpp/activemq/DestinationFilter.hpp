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
#ifndef ActiveMQ_DestinationFilter_hpp_
#define ActiveMQ_DestinationFilter_hpp_

#include "activemq/command/ActiveMQMessage.hpp"
#include "activemq/command/ActiveMQDestination.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    using namespace ifr;
    using namespace apache::activemq::command;

/*
 * 
 */
class DestinationFilter
{
public:
    const static char* ANY_DESCENDENT ;
    const static char* ANY_CHILD ;

public:
    DestinationFilter() ;
    virtual ~DestinationFilter() ;

    virtual bool matches(p<ActiveMQMessage> message) ;
    virtual bool matches(p<ActiveMQDestination> destination) = 0 ;
};

/* namespace */
  }
}

#endif /*ActiveMQ_DestinationFilter_hpp_*/
