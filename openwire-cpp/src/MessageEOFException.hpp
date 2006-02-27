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
#ifndef MessageEOFException_hpp_
#define MessageEOFException_hpp_

#include "TraceException.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace std ;

/*
 * Signals that an unexpected end of stream has been reached when reading
 * a message.
 */
class MessageEOFException : public TraceException
{
public:
    MessageEOFException() : TraceException()
       { /* no-op */ } ;
    MessageEOFException(const char *const& msg) : TraceException(msg)
       { /* no-op */ } ;
    MessageEOFException(const char* fileName, int lineNo, const char* msg) : TraceException(msg)
       { /* no-op */ } ;
} ;

/* namespace */
    }
  }
}

#endif /*MessageEOFException_hpp_*/