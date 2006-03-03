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
#ifndef FutureResponse_hpp_
#define FutureResponse_hpp_

#include <string>
#include "command/Response.hpp"
#include "util/ifr/p.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace ifr;
      using namespace apache::activemq::client::command;

/*
 * Interface for commands.
 */
class FutureResponse
{
private:
    p<Response> response ;
    bool        complete ;

public:
    FutureResponse() ;
    virtual ~FutureResponse() ;

    virtual p<Response> getResponse() ;
    virtual void setResponse(p<Response> response) ;
    virtual bool isCompleted() ;

    // TODO: add notify/wait
} ;

/* namespace */
    }
  }
}

#endif /*FutureResponse_hpp_*/
