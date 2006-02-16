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
#include "BrokerException.hpp"

using namespace apache::activemq::client;

/*
 * 
 */
BrokerException::BrokerException(p<BrokerError> cause)
   : OpenWireException("")  // TODO: Add trace
{
    string message ;

    brokerError = cause ;

    // Build exception message
    message.assign("The operation failed: Type: ") ;
    message.append( cause->getExceptionClass()->c_str() ) ;
    message.append(" stack: ") ;
    message.append( cause->getStackTrace()->c_str() ) ;
}

BrokerException::~BrokerException()
{
    // no-op
}

p<BrokerError> BrokerException::getCause()
{
    return brokerError ;
}
