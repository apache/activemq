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
#ifndef Connection_hpp_
#define Connection_hpp_

#include <list>
#include <string>

#include "ISession.hpp"
#include "IConnection.hpp"
#include "Session.hpp"
#include "OpenWireException.hpp"
#include "ConnectionClosedException.hpp"
#include "command/ExceptionResponse.hpp"
#include "command/ConnectionInfo.hpp"
#include "command/SessionInfo.hpp"
#include "command/SessionId.hpp"
#include "transport/ITransport.hpp"
#include "util/SimpleMutex.hpp"
#include "util/ifr/p"

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      using namespace std;
      using namespace ifr;
      using namespace apache::activemq::client::command;
      using namespace apache::activemq::client::transport;
      using namespace apache::activemq::client::util;

/*
 * 
 */
class Connection : public IConnection
{
private:
    p<ConnectionInfo>   connectionInfo ;
    p<ITransport>       transport ;
    list< p<ISession> > sessions ;
    bool                transacted,
                        connected,
                        closed ;
    AcknowledgementMode acknowledgementMode ;
    long                sessionCounter ;
    SimpleMutex         mutex ;

public:
    // Constructors
    Connection(p<ITransport> transport, p<ConnectionInfo> connectionInfo) ;
    ~Connection() ;

    // Attribute methods
    virtual AcknowledgementMode getAcknowledgementMode() ;
    virtual void setAcknowledgementMode(AcknowledgementMode mode) ;
    virtual p<string> getClientId() ;
    virtual void setClientId(const char* value) ;
    virtual bool getTransacted() ;
    virtual void setTransacted(bool tx) ;
    virtual p<ITransport> getTransport() ;
    virtual void setTransport(p<ITransport> transport) throw(OpenWireException) ;

    // Operation methods
    virtual p<ISession> createSession() throw(OpenWireException) ;
    virtual p<ISession> createSession(bool transacted, AcknowledgementMode mode) throw(OpenWireException) ;
    virtual p<Response> syncRequest(p<ICommand> command) throw(OpenWireException) ;

protected:
    // Implementation methods
    p<SessionInfo> createSessionInfo(bool transacted, AcknowledgementMode mode) ;
    void checkConnected() throw(OpenWireException) ;
} ;

/* namespace */
    }
  }
}

#endif /*Connection_hpp_*/