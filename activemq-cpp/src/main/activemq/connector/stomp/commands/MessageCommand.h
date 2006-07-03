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

#ifndef ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_MESSAGECOMMAND_H_
#define ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_MESSAGECOMMAND_H_

#include <cms/Message.h>
#include <activemq/connector/stomp/commands/StompMessage.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace commands{
        
    /**
     * Message command which represents a ActiveMQMessage with no body
     * can be sent or recieved.
     */
    class MessageCommand : public StompMessage< cms::Message >
    {
    public:

        MessageCommand(void) :
            StompMessage< cms::Message >() {
                initialize( getFrame() );
        }
        MessageCommand( StompFrame* frame ) : 
            StompMessage< cms::Message >( frame ) {
                validate( getFrame() );
        }
        virtual ~MessageCommand(void) {}

        /**
         * Clonse this message exactly, returns a new instance that the
         * caller is required to delete.
         * @return new copy of this message
         */
        virtual cms::Message* clone(void) const {
            StompFrame* frame = getFrame().clone();
            
            return new MessageCommand( frame );
        }

    };
    
}}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_COMMANDS_MESSAGECOMMAND_H_*/
