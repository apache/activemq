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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_STOMPCOMMAND_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_STOMPCOMMAND_H_

#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <activemq/connector/stomp/marshal/Marshalable.h>
#include <activemq/connector/stomp/marshal/MarshalException.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class StompCommand : public marshal::Marshalable
    {
    protected:
        
        /**
         * Inheritors are required to override this method to init the
         * frame with data appropriate for the command type.
         * @param frame Frame to init
         */
        virtual void initialize( StompFrame& frame ) = 0;

        /**
         * Inheritors are required to override this method to validate 
         * the passed stomp frame before it is marshalled or unmarshaled
         * @param frame Frame to validate
         * @returns true if frame is valid
         */
        virtual bool validate( const StompFrame& frame ) const = 0;

    public:

    	virtual ~StompCommand(void) {}

        /**
         * Sets the Command Id of this Message
         * @param id Command Id
         */
        virtual void setCommandId( const unsigned int id ) = 0;

        /**
         * Gets the Command Id of this Message
         * @return Command Id
         */
        virtual unsigned int getCommandId(void) const = 0;
        
        /**
         * Set if this Message requires a Response
         * @param required true if response is required
         */
        virtual void setResponseRequired( const bool required ) = 0;
        
        /**
         * Is a Response required for this Command
         * @return true if a response is required.
         */
        virtual bool isResponseRequired(void) const = 0;
        
        /**
         * Gets the Correlation Id that is associated with this message
         * @return the Correlation Id
         */
        virtual unsigned int getCorrelationId(void) const = 0;
        
        /**
         * Sets the Correlation Id if this Command
         * @param corrId Id
         */
        virtual void setCorrelationId( const unsigned int corrId ) = 0;
        
        /**
         * Get the Transaction Id of this Command
         * @return the Id of the Transaction
         */      
        virtual const char* getTransactionId(void) const = 0;
      
        /**
         * Set the Transaction Id of this Command
         * @param id the Id of the Transaction
         */
        virtual void setTransactionId( const std::string& id ) = 0;
        
        /**
         * Retrieve the Stomp Command Id for this message.
         * @return Stomp CommandId enum
         */
        virtual CommandConstants::CommandId getStompCommandId(void) const = 0;
        
        /**
         * Retrieves the Properties that are part of this command
         * @return const reference to a properties object
         */
        virtual util::Properties& getProperties(void) = 0;
        virtual const util::Properties& getProperties(void) const = 0;

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_STOMPCOMMAND_H_*/
