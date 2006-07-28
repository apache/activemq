/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "activemq/transport/TransportFilter.hpp"

using namespace apache::activemq::transport;


// --- Constructors -------------------------------------------------

/*
 * 
 */
TransportFilter::TransportFilter(p<ITransport> next)
{
    this->next     = next ;
    this->listener = NULL ;

    // Set us up as the command listener for next link in chain
    next->setCommandListener( smartify(this) ) ;
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
void TransportFilter::setCommandListener(p<ICommandListener> listener)
{
    this->listener = listener ;
}

/*
 *
 */
p<ICommandListener> TransportFilter::getCommandListener()
{
    return this->listener ;
}


// --- Operation methods --------------------------------------------

/*
 * 
 */
void TransportFilter::start()
{
    if( listener == NULL )
	    throw InvalidOperationException ("Command listener cannot be null when Start is called.") ;

    // Start next link in chain
    this->next->start() ;
}

/*
 * 
 */
void TransportFilter::oneway(p<BaseCommand> command)
{
    this->next->oneway(command) ;
}

/*
 * 
 */
p<FutureResponse> TransportFilter::asyncRequest(p<BaseCommand> command)
{
    return this->next->asyncRequest(command) ;
}

/*
 * 
 */
p<Response> TransportFilter::request(p<BaseCommand> command)
{
    return this->next->request(command) ;
}

// --- Event methods ------------------------------------------------

/*
 * 
 */
void TransportFilter::onCommand(p<ITransport> transport, p<BaseCommand> command)
{
    // Forward incoming command to "real" listener
    this->listener->onCommand(transport, command) ;
}

/*
 * 
 */
void TransportFilter::onError(p<ITransport> transport, exception& error)
{
    // Forward incoming exception to "real" listener
    this->listener->onError(transport, error) ;
}
