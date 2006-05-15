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
#include "activemq/transport/MutexFilter.hpp"

using namespace apache::activemq::transport;

// --- Constructors -------------------------------------------------

/*
 * 
 */
MutexFilter::MutexFilter(p<ITransport> next) :
   TransportFilter(next)
{
    this->next = next ;
}

/*
 * 
 */
MutexFilter::~MutexFilter()
{
    // Wait for transmission lock before disposal
    LOCKED_SCOPE (mutex) ;
}


// --- Operation methods --------------------------------------------

/*
 * 
 */
void MutexFilter::oneway(p<BaseCommand> command)
{
    // Wait for transmission lock and then transmit command
    LOCKED_SCOPE (mutex) ;
    this->next->oneway(command) ;
}

/*
 * 
 */
p<FutureResponse> MutexFilter::asyncRequest(p<BaseCommand> command)
{
    // Wait for transmission lock and then transmit command
    LOCKED_SCOPE (mutex) ;
    return this->next->asyncRequest(command) ;
}

/*
 * 
 */
p<Response> MutexFilter::request(p<BaseCommand> command)
{
    // Wait for transmission lock and then transmit command
    LOCKED_SCOPE (mutex) ;
    return this->next->request(command) ;
}
