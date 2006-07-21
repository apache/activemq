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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_TEXTMESSAGECOMMAND_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_TEXTMESSAGECOMMAND_H_

#include <cms/TextMessage.h>
#include <activemq/connector/stomp/commands/StompMessage.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{

    class TextMessageCommand : public StompMessage< cms::TextMessage >
    {
    public:

        TextMessageCommand(void) :
            StompMessage< cms::TextMessage >() {
                initialize( getFrame() );
        }
        TextMessageCommand( StompFrame* frame ) : 
            StompMessage< cms::TextMessage >( frame ) {
                validate( getFrame() );
        }
    	virtual ~TextMessageCommand(void) {}

        /**
         * Clonse this message exactly, returns a new instance that the
         * caller is required to delete.
         * @return new copy of this message
         */
        virtual cms::Message* clone(void) const {
            StompFrame* frame = getFrame().clone();
            
            return new TextMessageCommand( frame );
        }   

        /**
         * Gets the message character buffer.
         * @return The message character buffer.
         */
        virtual const char* getText(void) const throw( cms::CMSException ) {
            return getBytes();
        }
        
        /**
         * Sets the message contents.
         * @param msg The message buffer.
         */
        virtual void setText( const char* msg ) throw( cms::CMSException ) {
            setBytes( msg, strlen(msg) + 1, false );
        }

        /**
         * Sets the message contents.
         * @param msg The message buffer.
         */
        virtual void setText( const std::string& msg ) throw( cms::CMSException ) {
            setBytes( msg.c_str(), msg.length() + 1, false );
        }

    };

}}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_TEXTMESSAGECOMMAND_H_*/
