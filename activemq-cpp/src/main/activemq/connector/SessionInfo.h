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
#ifndef _ACTIVEMQ_CONNECTOR_SESSIONINFO_H_
#define _ACTIVEMQ_CONNECTOR_SESSIONINFO_H_

#include <activemq/connector/ConnectorResource.h>
#include <activemq/connector/TransactionInfo.h>
#include <cms/Session.h>

namespace activemq{
namespace connector{

    class SessionInfo : public ConnectorResource
    {
    public:

        /**
         * Destructor
         */
   	    virtual ~SessionInfo(void) {}
        
        /**
         * Gets the Connection Id of the Connection that this consumer is
         * using to receive its messages.
         * @return string value of the connection id
         */
        virtual const std::string& getConnectionId(void) const = 0;
   
        /**
         * Sets the Connection Id of the Connection that this consumer is
         * using to receive its messages.
         * @param string value of the connection id
         */
        virtual void setConnectionId( const std::string& id ) = 0;
        
        /**
         * Gets the Sessions Id value
         * @return id for this session
         */
        virtual unsigned int getSessionId(void) const = 0;

        /**
         * Sets the Session Id for this Session
         * @param integral id value for this session
         */
        virtual void setSessionId( const unsigned int id ) = 0;

        /**
         * Sets the Ack Mode of this Session Info object
         * @param Ack Mode
         */
        virtual void setAckMode(cms::Session::AcknowledgeMode ackMode) = 0;
        
        /**
         * Gets the Ack Mode of this Session
         * @return Ack Mode
         */
        virtual cms::Session::AcknowledgeMode getAckMode(void) const = 0;
        
        /**
         * Gets the currently active transaction info, if this session is
         * transacted, returns NULL when not transacted.  You must call
         * getAckMode and see if the session is transacted.
         * @return Transaction Id of current Transaction
         */
        virtual const TransactionInfo* getTransactionInfo(void) const = 0;
        
        /**
         * Sets the current transaction info for this session, this is nit
         * used when the session is not transacted.
         * @param Transaction Id
         */        
        virtual void setTransactionInfo( const TransactionInfo* transaction ) = 0;
        
    };

}}

#endif /*_ACTIVEMQ_CONNECTOR_SESSIONINFO_H_*/
