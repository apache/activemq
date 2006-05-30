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
#include "activemq/transport/CorrelatorFilter.hpp"

using namespace apache::activemq::transport;


// --- Constructors -------------------------------------------------

/*
 * 
 */
CorrelatorFilter::CorrelatorFilter(p<ITransport> next) :
   TransportFilter(next)
{
    this->next          = next ;
    this->nextCommandId = 0 ;
}


// --- Operation methods --------------------------------------------

/*
 * 
 */
void CorrelatorFilter::oneway(p<ICommand> command)
{
    // Set command id and that no response is required
    command->setCommandId( getNextCommandId() ) ;
    command->setResponseRequired(false) ;

    this->next->oneway(command) ;
}

/*
 * 
 */
p<FutureResponse> CorrelatorFilter::asyncRequest(p<ICommand> command)
{
    // Set command id and that a response is required
    command->setCommandId( getNextCommandId() ) ;
    command->setResponseRequired(true) ;

    // Register a future response holder with the command id
    p<FutureResponse> future = new FutureResponse() ;
    requestMap[command->getCommandId()] = future ;

    // Transmit command
    this->next->oneway(command) ;

    return future ;
}

/*
 * 
 */
p<Response> CorrelatorFilter::request(p<ICommand> command)
{
    p<FutureResponse> future = asyncRequest(command) ;
    p<Response> response = future->getResponse() ;

    if( response == NULL )
    {
        p<BrokerError> brokerError = new BrokerError() ;
        brokerError->setMessage("Timed out waiting for response from broker") ;
        throw BrokerException(brokerError) ;
    }
    else if ( response->getDataStructureType() == ExceptionResponse::TYPE )
    {
        p<ExceptionResponse> er = p_cast<ExceptionResponse> (response) ;
        p<BrokerError> brokerError = er->getException() ;
        throw BrokerException(brokerError) ;
    }
    return response ;
}


// --- Event methods ------------------------------------------------

/*
 * 
 */
void CorrelatorFilter::onCommand(p<ITransport> transport, p<ICommand> command)
{
    if( command->getDataStructureType() == Response::TYPE )
    {
        p<Response>       response = p_cast<Response>(command) ;
        p<FutureResponse> future = requestMap[response->getCorrelationId()] ;

        if( future != NULL )
        {
            if( response->getDataStructureType() == ExceptionResponse::TYPE )
            {
                p<ExceptionResponse> er    = p_cast<ExceptionResponse> (response) ;
                p<BrokerError> brokerError = er->getException() ;
                
                if( listener != NULL )
                {
                    BrokerException brokerException = BrokerException(brokerError) ;
                    listener->onError(smartify(this), brokerException) ;
                }
            }
            future->setResponse(response) ;
        }
        else
            cout << "Unknown response ID: " << response->getCorrelationId() << endl ;
    }
    else
    {
        if( listener != NULL )
            listener->onCommand(smartify(this), command) ;
        else
            cout << "ERROR: No handler available to process command: " << command->getDataStructureType() << endl ;
    }
}


// --- Implementation methods ---------------------------------------

/*
 * 
 */
int CorrelatorFilter::getNextCommandId()
{
    // Wait for lock and then fetch next command id
    LOCKED_SCOPE (mutex);
    return (short) ++nextCommandId ;
}
