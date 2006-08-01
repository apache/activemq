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
#ifndef _ACTIVEMQ_CONNECTOR_TRANSACTIONINFO_H_
#define _ACTIVEMQ_CONNECTOR_TRANSACTIONINFO_H_

#include <activemq/connector/ConnectorResource.h>

namespace activemq{
namespace connector{

    class SessionInfo;

    class TransactionInfo : public ConnectorResource
    {
    public:
   
   	    virtual ~TransactionInfo(void) {}
        
        /**
         * Gets the Transction Id
         * @return unsigned int Id
         */
        virtual unsigned int getTransactionId(void) const = 0;

        /**
         * Sets the Transction Id
         * @param id unsigned int Id
         */
        virtual void setTransactionId( const unsigned int id ) = 0;

        /**
         * Gets the Session Info that this transaction is attached too
         * @return SessionnInfo pointer
         */
        virtual const SessionInfo* getSessionInfo(void) const = 0;

        /**
         * Gets the Session Info that this transaction is attached too
         * @return session SessionnInfo pointer
         */
        virtual void setSessionInfo( const SessionInfo* session ) = 0;

    };

}}

#endif /*_ACTIVEMQ_CONNECTOR_TRANSACTIONINFO_H_*/
