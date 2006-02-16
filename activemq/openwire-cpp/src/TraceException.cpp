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
#include "TraceException.hpp"

using namespace apache::activemq::client;

/*
 * 
 */
TraceException::TraceException()
   : exception()
{
    // no-op
} ;

/*
 * 
 */
TraceException::TraceException(const char* msg)
   : exception(msg)
{
    // no-op
} ;

/*
 * 
 */
TraceException::TraceException(const char* fileName, int lineNo, const char* message)
    : exception(message)
{
    char buf[10] ;

    trace = new string() ;
    trace->append(fileName) ;
    trace->append(" at ") ;
    trace->append( itoa(lineNo, buf, 10) ) ;
}

TraceException::~TraceException()
{
    // no-op
}

p<string> TraceException::where()
{
    return trace ;
}
