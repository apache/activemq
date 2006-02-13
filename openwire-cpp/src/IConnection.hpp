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
#ifndef IConnection_hpp_
#define IConnection_hpp_

#include "ISession.hpp"
#include "OpenWireException.hpp"
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
        using namespace ifr;

enum AcknowledgementMode {
    UnknownMode, AutoMode, ClientMode, TransactionalMode 
} ;

/*
 * 
 */
struct IConnection
{
    virtual p<ISession> createSession() throw(OpenWireException) = 0 ;
    virtual p<ISession> createSession(bool transacted, AcknowledgementMode ackMode) throw(OpenWireException) = 0 ;
    virtual bool getTransacted() = 0 ;
    virtual void setTransacted(bool tx) = 0 ;
    virtual AcknowledgementMode getAcknowledgementMode() = 0 ;
    virtual void setAcknowledgementMode(AcknowledgementMode mode) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*IConnection_hpp_*/