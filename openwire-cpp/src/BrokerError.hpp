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
#ifndef BrokerError_hpp_
#define BrokerError_hpp_

#include "util/ifr/p.hpp"

#include <string>

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace ifr ;
      using namespace std ;

/*
 * 
 */
class BrokerError
{
private:
    p<string> exceptionClass ;
    p<string> stackTrace ;

public:
    BrokerError() ;
    virtual ~BrokerError() ;

    virtual p<string> getExceptionClass() ;
    virtual void setExceptionClass(const char* exceptionClass) ;
    virtual p<string> getStackTrace() ;
    virtual void setStackTrace(const char* exceptionClass) ;
} ;

/* namespace */
    }
  }
}

#endif /*BrokerError_hpp_*/
