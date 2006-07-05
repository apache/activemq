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
#ifndef ACTIVEMQ_CONNECTOR_STOMPTRANSACTIONINFO_H_
#define ACTIVEMQ_CONNECTOR_STOMPTRANSACTIONINFO_H_

#include <activemq/connector/TransactionInfo.h>
#include <activemq/connector/SessionInfo.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompTransactionInfo : public connector::TransactionInfo
    {
    private:
    
        // Transaction Id
        unsigned int transactionId;
        
        // Session Info - We do not own this
        const SessionInfo* session;
        
    public:

        /**
         * TransactionInfo Constructor
         */
        StompTransactionInfo(void) {
            transactionId = 0;
            session = NULL;
        }

        /**
         * Destructor
         */
        virtual ~StompTransactionInfo(void) {}

        /**
         * Gets the Transction Id
         * @return unsigned int Id
         */
        virtual unsigned int getTransactionId(void) const {
            return transactionId;
        }

        /**
         * Sets the Transction Id
         * @param unsigned int Id
         */
        virtual void setTransactionId( const unsigned int id ) {
            this->transactionId = id;
        } 

        /**
         * Gets the Session Info that this Transction is attached too
         * @return SessionnInfo pointer
         */
        virtual const SessionInfo* getSessionInfo(void) const {
            return session;
        }
        
        /**
         * Gets the Session Info that this Transction is attached too
         * @return SessionnInfo pointer
         */
        virtual void setSessionInfo( const SessionInfo* session ) {
            this->session = session;
        }

    };

}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMPTRANSACTIONINFO_H_*/
