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
#ifndef ActiveMQ_ITransport_hpp_
#define ActiveMQ_ITransport_hpp_

#include "cms/IStartable.hpp"
#include "activemq/ICommand.hpp"
#include "activemq/command/Response.hpp"
#include "activemq/transport/FutureResponse.hpp"
#include "activemq/transport/ICommandListener.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr ;
      using namespace apache::cms;
      using namespace apache::activemq;
      using namespace apache::activemq::command;

/*
 * Represents the logical networking transport layer.
 */
struct ITransport : IStartable
{
    virtual void setCommandListener(p<ICommandListener> listener) = 0 ;
    virtual p<ICommandListener> getCommandListener() = 0 ;

    virtual void oneway(p<ICommand> command) = 0 ;
	virtual p<FutureResponse> asyncRequest(p<ICommand> command) = 0 ;
	virtual p<Response> request(p<ICommand> command) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_ITransport_hpp_*/
