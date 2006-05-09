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
#ifndef ActiveMQ_BrokerError_hpp_
#define ActiveMQ_BrokerError_hpp_

#include <string>
#include <ostream>
#include <sstream>
#include "activemq/command/AbstractCommand.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace command
    {
      using namespace ifr ;
      using namespace std ;

/*
 * 
 */
struct StackTraceElement
{
    p<string> className ;
    p<string> fileName ;
    p<string> methodName ;
    int lineNumber ;
} ;

/*
 * Represents an exception on the broker.
 */
class BrokerError : public AbstractCommand
{
private:
    p<string>             message ;
    p<string>             exceptionClass ;
    array<StackTraceElement> stackTraceElements ;
    p<BrokerError>        cause ;

public:
    BrokerError() ;

    virtual p<string> getMessage() ;
    virtual void setMessage(const char* msg) ;
    virtual p<string> getExceptionClass() ;
    virtual void setExceptionClass(const char* exClass) ;
    virtual array<StackTraceElement> getStackTraceElements() ;
    virtual void setStackTraceElements(array<StackTraceElement> elements) ;
    virtual p<BrokerError> getCause() ;
    virtual void setCause(p<BrokerError> cause) ;
    virtual p<string> getStackTrace() ;
    virtual void printStackTrace(ostream& out) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_BrokerError_hpp_*/
