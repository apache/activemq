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
#ifndef _ACTIVEMQ_CONNECTOR_CONSUMERINFO_H_
#define _ACTIVEMQ_CONNECTOR_CONSUMERINFO_H_

#include <activemq/connector/ConnectorResource.h>
#include <activemq/connector/SessionInfo.h>
#include <cms/Destination.h>
#include <string>

namespace activemq{
namespace connector{

    class ConsumerInfo : public ConnectorResource
    {
    public:

        virtual ~ConsumerInfo(void) {}
      
        /**
         * Gets this message consumer's message selector expression.
         * @return This Consumer's selector expression or "".
         */
        virtual const std::string& getMessageSelector(void) const = 0;
        
        /**
         * Sets this message consumer's message selector expression.
         * @param This Consumer's selector expression or "".
         */
        virtual void setMessageSelector( const std::string& selector ) = 0;        

        /**
         * Gets the ID that is assigned to this consumer
         * @return value of the Consumer Id.
         */
        virtual unsigned int getConsumerId(void) const = 0;
        
        /**
         * Sets the ID that is assigned to this consumer
         * @return string value of the Consumer Id.
         */
        virtual void setConsumerId( const unsigned int id ) = 0;
        
        /**
         * Gets the Destination that this Consumer is subscribed on
         * @return Destination
         */
        virtual const cms::Destination& getDestination(void) const = 0;
        
        /**
         * Sets the destination that this Consumer is listening on
         * @param Destination
         */
        virtual void setDestination( const cms::Destination& destination ) = 0;

        /**
         * Gets the Session Info that this consumer is attached too
         * @return SessionnInfo pointer
         */
        virtual const SessionInfo* getSessionInfo(void) const = 0;

        /**
         * Gets the Session Info that this consumer is attached too
         * @return SessionnInfo pointer
         */
        virtual void setSessionInfo( const SessionInfo* session ) = 0;

    };

}}

#endif /*_ACTIVEMQ_CONNECTOR_CONSUMERINFO_H_*/
