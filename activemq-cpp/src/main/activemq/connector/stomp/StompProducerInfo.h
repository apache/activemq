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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPPRODUCERINFO_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPPRODUCERINFO_H_

#include <activemq/connector/ProducerInfo.h>
#include <cms/Destination.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompProducerInfo : public ProducerInfo
    {
    private:
    
        // Producer Id
        unsigned int producerId;

        // Default Destination
        cms::Destination* dest;
        
        // Session that this producer is attached to - we do not own this
        const SessionInfo* session;
        
    public:

    	StompProducerInfo(void) { dest = NULL; }
    	virtual ~StompProducerInfo(void) { delete dest; }

        /**
         * Retrieves the default destination that this producer
         * sends its messages to.
         * @return Destionation, owned by this object
         */
        virtual const cms::Destination& getDestination(void) const {
            return *dest;
        }

        /**
         * Sets the Default Destination for this Producer
         * @param reference to a destination, copied internally
         */
        virtual void setDestination( const cms::Destination& dest ) {
            this->dest = dest.clone();
        }

        /**
         * Gets the ID that is assigned to this Producer
         * @return value of the Producer Id.
         */
        virtual unsigned int getProducerId(void) const {
            return producerId;
        }
        
        /**
         * Sets the ID that is assigned to this Producer
         * @return string value of the Producer Id.
         */
        virtual void setProducerId( const unsigned int id ) {
            this->producerId = id;
        }

        /**
         * Gets the Session Info that this consumer is attached too
         * @return SessionnInfo pointer
         */
        virtual const SessionInfo* getSessionInfo(void) const {
            return session;
        }

        /**
         * Gets the Session Info that this consumer is attached too
         * @return SessionnInfo pointer
         */
        virtual void setSessionInfo( const SessionInfo* session ) {
            this->session = session;
        }

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPPRODUCERINFO_H_*/
