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
#ifndef Ppr_IOException_hpp_
#define Ppr_IOException_hpp_

#include "ppr/TraceException.hpp"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace apache::ppr;
      using namespace std ;

/*
 * Signals that an I/O exception of some sort has occurred.
 */
class IOException : public TraceException
{
public:
    IOException() : TraceException()
    { /* no-op */ } ;
    IOException(const char* msg, ...)
    {
        va_list vargs ;

        va_start(vargs, msg) ;
        TraceException::buildMessage(msg, vargs) ;
    }
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_IOException_hpp_*/
