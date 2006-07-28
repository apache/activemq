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
#ifndef ActiveMQ_FutureResponse_hpp_
#define ActiveMQ_FutureResponse_hpp_

#include <string>
#include "activemq/command/Response.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/thread/Semaphore.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr;
      using namespace apache::activemq::command;
      using namespace apache::ppr::thread;

/*
 * Interface for commands.
 */
class FutureResponse
{
private:
    p<Response>    response ;
    p<SimpleMutex> mutex ;
    p<Semaphore>   semaphore ;
    int            maxWait ;
    bool           complete ;

public:
    FutureResponse() ;
    virtual ~FutureResponse() {}

    virtual p<Response> getResponse() ;
    virtual void setResponse(p<Response> response) ;
    virtual p<Response> getAsyncState() ;
    virtual void setAsyncState(p<Response> response) ;
    virtual p<SimpleMutex> getAsyncWaitHandle() ; // BUG: Shouldn't we return the semaphore here? What is it needed for? SHouldn't we require to use getResponse() instead? //dafah
    virtual bool isCompleted() ;
    virtual bool getCompletedSynchronously() ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_FutureResponse_hpp_*/
