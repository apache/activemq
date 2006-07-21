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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_BYTESMESSAGECOMMAND_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_BYTESMESSAGECOMMAND_H_

#include <cms/BytesMessage.h>
#include <activemq/connector/stomp/commands/StompMessage.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    /**
     * Implements the interface for a cms::BytesMessage.  Uses the template
     * class StompMessage to implement all cms::Message type functionality
     * and implements the BytesMessage interface here.
     */    
    class BytesMessageCommand : public StompMessage< cms::BytesMessage >
    {
    public:

        BytesMessageCommand(void) :
            StompMessage< cms::BytesMessage >() {
                initialize( getFrame() );
        }
        BytesMessageCommand( StompFrame* frame ) : 
            StompMessage< cms::BytesMessage >( frame ) {
                validate( getFrame() );
        }
    	virtual ~BytesMessageCommand(void) {}

        /**
         * Clonse this message exactly, returns a new instance that the
         * caller is required to delete.
         * @return new copy of this message
         */
        virtual cms::Message* clone(void) const {
            StompFrame* frame = getFrame().clone();
            
            return new BytesMessageCommand( frame );
        }   

        /**
         * sets the bytes given to the message body.  
         * @param Byte Buffer to copy
         * @param Number of bytes in Buffer to copy
         * @throws CMSException
         */
        virtual void setBodyBytes( const unsigned char* buffer, 
                                   const unsigned long numBytes ) 
            throw( cms::CMSException ) {
            this->setBytes(
                reinterpret_cast<const char*>( buffer ), numBytes );
        }
        
        /**
         * Gets the bytes that are contained in this message, user should
         * copy this data into a user allocated buffer.  Call 
         * <code>getBodyLength</code> to determine the number of bytes
         * to expect.
         * @return const pointer to a byte buffer
         */
        virtual const unsigned char* getBodyBytes(void) const {
            return reinterpret_cast<const unsigned char*>( this->getBytes() );
        }
      
        /**
         * Returns the number of bytes contained in the body of this message.
         * @return number of bytes.
         */
        virtual unsigned long getBodyLength(void) const {
            return this->getNumBytes();
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_BYTESMESSAGECOMMAND_H_*/
