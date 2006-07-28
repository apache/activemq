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
#ifndef ActiveMQ_MutexFilter_hpp_
#define ActiveMQ_MutexFilter_hpp_

#include "activemq/transport/TransportFilter.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr;
      using namespace apache::ppr::thread;

/*
 * A filter transport which gaurds access to the next transport
 * using a mutex.
 */
class MutexFilter : public TransportFilter
{
protected:
    SimpleMutex mutex ;

public:
    MutexFilter(p<ITransport> next) ;
    virtual ~MutexFilter() ;

	virtual void oneway(p<ICommand> command) ;
	virtual p<FutureResponse> asyncRequest(p<ICommand> command) ;
	virtual p<Response> request(p<ICommand> command) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_MutexFilter_hpp_*/
