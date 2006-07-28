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
#ifndef ActiveMQ_Connection_hpp_
#define ActiveMQ_Connection_hpp_

#include <iostream>
#include <list>
#include <string>
#include <map>

#include "cms/ISession.hpp"
#include "cms/IConnection.hpp"
#include "cms/IExceptionListener.hpp"
#include "cms/CmsException.hpp"
#include "activemq/ConnectionClosedException.hpp"
#include "activemq/MessageConsumer.hpp"
#include "activemq/command/BrokerInfo.hpp"
#include "activemq/command/WireFormatInfo.hpp"
#include "activemq/command/ExceptionResponse.hpp"
#include "activemq/command/ConnectionInfo.hpp"
#include "activemq/command/LocalTransactionId.hpp"
#include "activemq/command/MessageDispatch.hpp"
#include "activemq/command/SessionInfo.hpp"
#include "activemq/command/SessionId.hpp"
#include "activemq/command/ShutdownInfo.hpp"
#include "activemq/transport/ITransport.hpp"
#include "activemq/transport/ICommandListener.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/util/ifr/p"

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

namespace apache
{
  namespace activemq
  {
    using namespace std;
    using namespace ifr;
    using namespace apache::cms;
    using namespace apache::activemq::command;
    using namespace apache::activemq::transport;
    using namespace apache::ppr::thread;
    using namespace apache::ppr::util;

/*
 * 
 */
class Connection : public IConnection, public ICommandListener
{
private:
    p<ConnectionInfo>     connectionInfo ;
    p<ITransport>         transport ;
    p<BrokerInfo>         brokerInfo ;        // from MQ broker
    p<WireFormatInfo>     brokerWireFormat ;  // from MQ broker
    p<IExceptionListener> listener ;
    list< p<ISession> >   sessions ;
    bool                  connected,
                          closed ;
    AcknowledgementMode   acknowledgementMode ;
    long long             sessionCounter,
                          tempDestinationCounter,
                          localTransactionCounter ;
    SimpleMutex           mutex ;

public:
    // Constructors
    Connection(p<ITransport> transport, p<ConnectionInfo> connectionInfo) ;
    virtual ~Connection() ;

    // Attribute methods
    virtual void setExceptionListener(p<IExceptionListener> listener) ;
    virtual p<IExceptionListener> getExceptionListener() ;
    virtual p<ITransport> getTransport() ;
    virtual void setTransport(p<ITransport> transport) ;
    virtual p<string> getClientId() ;
    virtual void setClientId(const char* value) throw (CmsException) ;
    virtual p<BrokerInfo> getBrokerInfo() ;
    virtual p<WireFormatInfo> getBrokerWireFormat() ;
    virtual AcknowledgementMode getAcknowledgementMode() ;
    virtual void setAcknowledgementMode(AcknowledgementMode mode) ;
//    virtual void addConsumer(p<ConsumerId> consumerId, p<MessageConsumer> messageConsumer) ;
//    virtual void removeConsumer(p<ConsumerId> consumerId) ;
    virtual p<ConnectionId> getConnectionId() ;

    // Operation methods
    virtual p<ISession> createSession() throw(CmsException) ;
    virtual p<ISession> createSession(AcknowledgementMode mode) throw(CmsException) ;
    virtual p<Response> syncRequest(p<ICommand> command) throw(CmsException) ;
    virtual void oneway(p<ICommand> command) throw(CmsException) ;
    virtual void disposeOf(p<IDataStructure> dataStructure) throw(CmsException) ;
    virtual p<string> createTemporaryDestinationName() ;
    virtual p<LocalTransactionId> createLocalTransactionId() ;
    virtual void close() ;

protected:
    // Implementation methods
    p<SessionInfo> createSessionInfo(AcknowledgementMode mode) ;
    void checkConnected() throw(CmsException) ;
    void onCommand(p<ITransport> transport, p<ICommand> command) ;
    void onError(p<ITransport> transport, exception& error) ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_Connection_hpp_*/
