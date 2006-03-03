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
#ifndef TraceException_hpp_
#define TraceException_hpp_

#include <exception>
#include <string>
#include "util/ifr/p.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace std ;
      using namespace ifr ;

/*
 * Base class for all exceptions containing trace information.
 */
class TraceException : public exception
{
    p<string> trace ;

public:
    TraceException() ;
    TraceException(const char* msg) ;
    TraceException(const char* fileName, int lineNo, const char* msg) ;
    virtual ~TraceException() ;

    p<string> where() ;
} ;

/* namespace */
    }
  }
}

#endif /*TraceException_hpp_*/