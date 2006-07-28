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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_RECEIPTCOMMAND_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_RECEIPTCOMMAND_H_

#include <activemq/connector/stomp/commands/AbstractCommand.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <activemq/transport/Response.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    /**
     * Message sent from the Broker when a receipt is requested
     * for messages that are sent.
     */
    class ReceiptCommand : public AbstractCommand< transport::Response >
    {
    public:
   
        ReceiptCommand(void) :
            AbstractCommand<transport::Response>() {
                initialize( getFrame() );
        }
        ReceiptCommand( StompFrame* frame ) : 
            AbstractCommand<transport::Response>( frame ) {
                validate( getFrame() );
        }
        virtual ~ReceiptCommand(void) {}

        /**
         * Get the receipt id
         */      
        virtual const char* getReceiptId(void) const{
            return getPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_RECEIPTID) );
        }

        /**
         * Set the receipt id
         */
        virtual void setReceiptId( const std::string& id ){
            setPropertyValue( 
                CommandConstants::toString( 
                    CommandConstants::HEADER_RECEIPTID ),
                id );
        }

    protected:
    
        /**
         * Inheritors are required to override this method to init the
         * frame with data appropriate for the command type.
         * @param Frame to init
         */
        virtual void initialize( StompFrame& frame )
        {
            frame.setCommand( CommandConstants::toString(
                CommandConstants::RECEIPT ) );
        }

        /**
         * Inheritors are required to override this method to validate 
         * the passed stomp frame before it is marshalled or unmarshaled
         * @param Frame to validate
         * @returns true if frame is valid
         */
        virtual bool validate( const StompFrame& frame ) const
        {
            if((frame.getCommand() == 
                CommandConstants::toString( CommandConstants::RECEIPT )) &&
               (frame.getProperties().hasProperty(
                    CommandConstants::toString( 
                        CommandConstants::HEADER_RECEIPTID ) ) ) )
            {
                return true;
            }

            return false;
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_RECEIPTCOMMAND_H_*/
