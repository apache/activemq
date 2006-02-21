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
#ifndef ExceptionResponse_hpp_
#define ExceptionResponse_hpp_

#include "BrokerError.hpp"
#include "command/Response.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr;
        using namespace apache::activemq::client;

/*
 * 
 */
class ExceptionResponse : public Response
{
private:
    p<BrokerError> exception ;

public:
    static const int TYPE = 31 ;

public:
    ExceptionResponse() ;
    ~ExceptionResponse() ;

    virtual int getCommandType() ;
    p<BrokerError> getException() ;
    void setException(p<BrokerError> exception) ;
};

/* namespace */
      }
    }
  }
}

#endif /*ExceptionResponse_hpp_*/