/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ActiveMQ_CorrelatorFilter_hpp_
#define ActiveMQ_CorrelatorFilter_hpp_

#include <iostream>
#include "activemq/BrokerException.hpp"
#include "activemq/command/Response.hpp"
#include "activemq/command/ExceptionResponse.hpp"
#include "activemq/transport/TransportFilter.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr;
      using namespace apache::activemq;
      using namespace apache::activemq::command;

/*
 * Interface for commands.
 */
class CorrelatorFilter : public TransportFilter
{
protected:
    SimpleMutex                  mutex ;
    map<int, p<FutureResponse> > requestMap ;
    int                          nextCommandId ;

public:
    CorrelatorFilter(p<ITransport> next) ;
    virtual ~CorrelatorFilter() {}

	virtual void oneway(p<ICommand> command) ;
	virtual p<FutureResponse> asyncRequest(p<ICommand> command) ;
	virtual p<Response> request(p<ICommand> command) ;

    virtual void onCommand(p<ITransport> transport, p<ICommand> command) ;

protected:
    virtual int getNextCommandId() ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_CorrelatorFilter_hpp_*/
