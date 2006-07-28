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
#include "activemq/Connection.hpp"
#include "activemq/Session.hpp"

using namespace apache::activemq;


// --- Constructors -------------------------------------------------

/*
 *
 */
Connection::Connection(p<ITransport> transport, p<ConnectionInfo> connectionInfo)
{
    this->transport               = transport ;
    this->connectionInfo          = connectionInfo ;
    this->acknowledgementMode     = AutoAckMode ;
    this->connected               = false ;
    this->closed                  = false ;
    this->brokerInfo              = NULL ;
    this->brokerWireFormat        = NULL ;
    this->sessionCounter          = 0 ;
    this->tempDestinationCounter  = 0 ;
    this->localTransactionCounter = 0 ;

    // Hook up as a command listener and start dispatching
    transport->setCommandListener(smartify(this)) ;
    transport->start() ;
}

/*
 *
 */
Connection::~Connection()
{
    // Make sure connection is closed
    close() ;
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
void Connection::setExceptionListener(p<IExceptionListener> listener)
{
    this->listener = listener ;
}

/*
 *
 */
p<IExceptionListener> Connection::getExceptionListener()
{
    return listener ;
}

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
void Connection::setTransport(p<ITransport> transport)
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
void Connection::setClientId(const char* value) throw (CmsException)
{
    if( connected )
        throw CmsException("You cannot change the client id once a connection is established") ; 

    p<string> clientId = new string(value) ;
    connectionInfo->setClientId( clientId ) ;
}


/*
 *
 */
p<BrokerInfo> Connection::getBrokerInfo()
{
    return brokerInfo ;
}

/*
 *
 */
p<WireFormatInfo> Connection::getBrokerWireFormat()
{
    return brokerWireFormat ;
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
void Connection::setAcknowledgementMode(AcknowledgementMode ackMode)
{
    acknowledgementMode = ackMode ;
}

/*
 *
 */
p<ConnectionId> Connection::getConnectionId()
{
    return connectionInfo->getConnectionId() ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
void Connection::close()
{
    if( !closed )
    {
        list< p<ISession> >::iterator sessionIter ;

        // Iterate through all sessions and close them down
        for( sessionIter = sessions.begin() ;
             sessionIter != sessions.end() ;
             sessionIter++ )
        {
            (*sessionIter)->close() ;
        }
        // Empty session list
        sessions.clear() ;

        // Remove connection from broker
        disposeOf( getConnectionId() ) ;

        // Finally, transmit a shutdown command to broker
        transport->oneway( new ShutdownInfo() ) ;
        closed = true ;
    }
}

/*
 *
 */
p<ISession> Connection::createSession() throw(CmsException)
{
    return createSession(acknowledgementMode) ;
}

/*
 *
 */
p<ISession> Connection::createSession(AcknowledgementMode ackMode) throw(CmsException)
{
    p<SessionInfo> sessionInfo = createSessionInfo( ackMode ) ;

    // Send session info to broker
    syncRequest(sessionInfo) ;

    p<ISession> session = new Session(smartify(this), sessionInfo, ackMode) ;
    sessions.push_back(session) ;

    return session ;
}

/*
 * Performs a synchronous request-response with the broker.
 */
p<Response> Connection::syncRequest(p<ICommand> command) throw(CmsException) 
{
    checkConnected() ;
    
    p<Response> response = transport->request(command) ;

    if( response->getDataStructureType() == ExceptionResponse::TYPE )
    {
        p<ExceptionResponse> exceptionResponse = p_cast<ExceptionResponse> (response) ;
        p<BrokerError>       brokerError = exceptionResponse->getException() ;
        string               message ;

        // Build error message
        message.assign("Request failed: ") ;
        message.append( brokerError->getExceptionClass()->c_str() ) ;
        message.append(", ") ;
        message.append( brokerError->getStackTrace()->c_str() ) ;

        // TODO: Change to CMSException()
        throw CmsException( message.c_str() ) ; 
    }
    return response ;
}

/*
 *
 */
void Connection::oneway(p<ICommand> command) throw(CmsException)
{
    checkConnected() ;
    transport->oneway(command) ;
}

/*
 *
 */
void Connection::disposeOf(p<IDataStructure> dataStructure) throw(CmsException)
{
    p<RemoveInfo> command = new RemoveInfo() ;
    command->setObjectId( dataStructure ) ;
    syncRequest(command) ;
}

/*
 * Creates a new temporary destination name.
 */
p<string> Connection::createTemporaryDestinationName()
{
    p<string> name = new string() ;
    char*     buffer = new char[15] ;

    {
        LOCKED_SCOPE( mutex );

        name->assign( connectionInfo->getConnectionId()->getValue()->c_str() ) ;
        name->append( ":" ) ;
#ifdef unix
        sprintf(buffer, "%lld", ++tempDestinationCounter) ;
#else
        sprintf(buffer, "%I64d", ++tempDestinationCounter) ;
#endif
        name->append( buffer ) ;
    }

    return name ;
}

/*
 * Creates a new local transaction ID.
 */
p<LocalTransactionId> Connection::createLocalTransactionId()
{
    p<LocalTransactionId> id = new LocalTransactionId() ;

    id->setConnectionId( getConnectionId() ) ;

    {
        LOCKED_SCOPE (mutex);
        id->setValue( ++localTransactionCounter ) ;
    }

    return id ;
}


// --- Implementation methods ---------------------------------------

/*
 *
 */
p<SessionInfo> Connection::createSessionInfo(AcknowledgementMode ackMode)
{
    p<SessionInfo> sessionInfo = new SessionInfo() ;
    p<SessionId>   sessionId   = new SessionId() ;
    
    sessionId->setConnectionId ( connectionInfo->getConnectionId()->getValue() ) ;

    {
        LOCKED_SCOPE( mutex );
        sessionId->setValue( ++sessionCounter ) ; 
    }

    sessionInfo->setSessionId( sessionId ) ;
    return sessionInfo ; 
}

/*
 *
 */
void Connection::checkConnected() throw(CmsException) 
{
    if( closed )
        throw ConnectionClosedException("Oops! Connection already closed.") ;

    if( !connected )
    {
        connected = true ;

        // Send the connection info and see if we get an ack/nak
        syncRequest( connectionInfo ) ;
    } 
}

/*
 * Handle incoming commands.
 */
void Connection::onCommand(p<ITransport> transport, p<ICommand> command)
{
    if( command->getDataStructureType() == MessageDispatch::TYPE )
    {
        p<MessageDispatch> dispatch = p_cast<MessageDispatch> (command) ;
        p<ConsumerId> consumerId = dispatch->getConsumerId() ;
        p<MessageConsumer> consumer = NULL ;
        list< p<ISession> >::const_iterator tempIter ;

        // Iterate through all sessions and check if one has the consumer
        for( tempIter = sessions.begin() ;
             tempIter != sessions.end() ;
             tempIter++ )
        {
            consumer = p_cast<Session> (*tempIter)->getConsumer(consumerId) ;

            // Found a match?
            if( consumer != NULL )
                break ;
        }
        if( consumer == NULL )
            cout << "ERROR: No such consumer active: " << consumerId->getValue() << endl ;
        else
        {
            p<ActiveMQMessage> message = p_cast<ActiveMQMessage> (dispatch->getMessage()) ;
            consumer->dispatch(message) ;
        }
    }
    else if( command->getDataStructureType() == WireFormatInfo::TYPE )
        this->brokerWireFormat = p_cast<WireFormatInfo> (command) ;

    else if( command->getDataStructureType() == BrokerInfo::TYPE )
        this->brokerInfo = p_cast<BrokerInfo> (command) ;

    else
        cout << "ERROR: Unknown command: " << command->getDataStructureType() << endl ;
}

/*
 * Handle incoming broker errors.
 */
void Connection::onError(p<ITransport> transport, exception& error)
{
    if( listener != NULL )
        this->listener->onException(error) ;
    else
        cout << "ERROR: Received a broker exception: " << error.what() << endl ;
}
