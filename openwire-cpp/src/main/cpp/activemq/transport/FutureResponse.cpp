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
#include "activemq/transport/FutureResponse.hpp"

using namespace apache::activemq::transport;

/*
 * 
 */
FutureResponse::FutureResponse()
{
    complete  = false ;
    response  = NULL ;
    maxWait   = 3 ;
    mutex     = new SimpleMutex() ;
    semaphore = new Semaphore() ;
}

p<Response> FutureResponse::getResponse()
{
    // Wait for response to arrive
    LOCKED_SCOPE (mutex);
    while ( response == NULL )
    {
        LOCKED_SCOPE_UNLOCK;
        semaphore->wait(maxWait); // BUG: Why have a max wait when what you do is just to wait again and again? //dafah
        LOCKED_SCOPE_RELOCK;
    }
    return response ;
}

void FutureResponse::setResponse(p<Response> response)
{
    {
        LOCKED_SCOPE (mutex);
        this->response = response ;
        complete       = true ;
    }
    // Signal that response has arrived
    semaphore->notify() ;
}

bool FutureResponse::isCompleted()
{
    return complete ;
}

bool FutureResponse::getCompletedSynchronously()
{
    return false ;
}

p<SimpleMutex> FutureResponse::getAsyncWaitHandle()
{
    return mutex ;
}

p<Response> FutureResponse::getAsyncState()
{
    return response ;
}

void FutureResponse::setAsyncState(p<Response> response)
{
    setResponse( response ) ;
}
