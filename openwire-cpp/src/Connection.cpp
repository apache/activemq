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
#include "Connection.hpp"

using namespace apache::activemq::client;


// --- Constructors -------------------------------------------------

/*
 *
 */
Connection::Connection(p<ITransport> transport, p<ConnectionInfo> connectionInfo)
{
    this->transport      = transport ;
    this->connectionInfo = connectionInfo ;
    this->connected      = false ;
    this->transacted     = false ;
    this->closed         = false ;
}

/*
 *
 */
Connection::~Connection()
{
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
p<ITransport> Connection::getTransport()
{
    return transport ;
}

/*
 *
 */
void Connection::setTransport(p<ITransport> transport) throw(OpenWireException) 
{
    this->transport = transport ;
}

/*
 *
 */
p<string> Connection::getClientId()
{
    return connectionInfo->getClientId() ;
}

/*
 *
 */
void Connection::setClientId(const char* value)
{
    if( connected )
        throw OpenWireException("You cannot change the ClientId once a connection is established") ; 

    connectionInfo->setClientId( value ) ;
}

/*
 *
 */
bool Connection::getTransacted()
{
    return transacted ;
}

/*
 *
 */
void Connection::setTransacted(bool tx)
{
    transacted = tx ;
}

/*
 *
 */
AcknowledgementMode Connection::getAcknowledgementMode()
{
    return acknowledgementMode ;
}

/*
 *
 */
void Connection::setAcknowledgementMode(AcknowledgementMode mode)
{
    acknowledgementMode = mode ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
p<ISession> Connection::createSession() throw(OpenWireException)
{
    return createSession(transacted, acknowledgementMode) ;
}

/*
 *
 */
p<ISession> Connection::createSession(bool transacted, AcknowledgementMode mode) throw(OpenWireException)
{
    checkConnected() ;

    p<SessionInfo> sessionInfo = createSessionInfo(transacted, acknowledgementMode) ;

    // Send session info to broker
    syncRequest(sessionInfo) ;

    p<ISession> session = new Session(this, sessionInfo) ;
    sessions.push_back(session) ;

    return session ;
}

/*
 *
 */
p<Response> Connection::syncRequest(p<ICommand> command) throw(OpenWireException) 
{
    checkConnected() ;
    
    p<Response> response = transport->request(command) ;

    if( response->getCommandType() == ExceptionResponse::TYPE )
    {
        p<ExceptionResponse> exceptionResponse = (p<ExceptionResponse>&)response ;
        p<BrokerError>       brokerError = exceptionResponse->getException() ;
        string               message ;

        // Build error message
        message.assign("Request failed: ") ;
        message.append( brokerError->getExceptionClass()->c_str() ) ;
        message.append(", ") ;
        message.append( brokerError->getStackTrace()->c_str() ) ;

        throw new OpenWireException( message.c_str() ) ; 
    }
    return response ;
}


// --- Implementation methods ---------------------------------------

/*
 *
 */
p<SessionInfo> Connection::createSessionInfo(bool transacted, AcknowledgementMode acknowledgementMode)
{
    p<SessionInfo> sessionInfo = new SessionInfo() ;
    p<SessionId>   sessionId   = new SessionId() ;
    
    sessionId->setConnectionId ( connectionInfo->getConnectionId()->getValue()->c_str() ) ;

    mutex.lock() ;
    sessionId->setValue( ++sessionCounter ) ; 
    mutex.unlock() ;

    sessionInfo->setSessionId( sessionId ) ;
    return sessionInfo ; 
}

/*
 *
 */
void Connection::checkConnected() throw(OpenWireException) 
{
    if( closed )
        throw new ConnectionClosedException("Oops! Connection already closed.") ;

    if( !connected )
    {
        syncRequest((p<ICommand>&)connectionInfo) ;
        connected = true ;
    } 
}
