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
#ifndef ActiveMQ_TransportFilter_hpp_
#define ActiveMQ_TransportFilter_hpp_

#include <string>
#include "activemq/command/BaseCommand.hpp"
#include "activemq/command/Response.hpp"
#include "activemq/transport/ITransport.hpp"
#include "ppr/InvalidOperationException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr;
      using namespace apache::activemq::command;
      using namespace apache::ppr;

/*
 * 
 */
class TransportFilter : public ITransport, public ICommandListener
{
protected:
    p<ITransport>       next ;
    p<ICommandListener> listener ;

public:
    TransportFilter(p<ITransport> next) ;
    virtual ~TransportFilter() {}

    virtual void setCommandListener(p<ICommandListener> listener) ;
    virtual p<ICommandListener> getCommandListener() ;

	virtual void start() ;
	virtual void oneway(p<BaseCommand> command) ;
	virtual p<FutureResponse> asyncRequest(p<BaseCommand> command) ;
	virtual p<Response> request(p<BaseCommand> command) ;

    virtual void onCommand(p<ITransport> transport, p<BaseCommand> command) ;
    virtual void onError(p<ITransport> transport, exception& error) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_TransportFilter_hpp_*/
