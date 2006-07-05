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

#include <activemq/connector/stomp/marshal/Marshaler.h>

#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/marshal/MarshalException.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <activemq/connector/stomp/commands/AbstractCommand.h>
#include <activemq/connector/stomp/StompFrame.h>

// Commands we can receive
#include <activemq/connector/stomp/commands/ConnectedCommand.h>
#include <activemq/connector/stomp/commands/ReceiptCommand.h>
#include <activemq/connector/stomp/commands/ErrorCommand.h>

// Message Commands we can receive
#include <activemq/connector/stomp/commands/MessageCommand.h>
#include <activemq/connector/stomp/commands/BytesMessageCommand.h>
#include <activemq/connector/stomp/commands/TextMessageCommand.h>

using namespace activemq;
using namespace activemq::exceptions;
using namespace activemq::transport;
using namespace activemq::connector::stomp;
using namespace activemq::connector::stomp::commands;
using namespace activemq::connector::stomp::marshal;

////////////////////////////////////////////////////////////////////////////////      
transport::Command* Marshaler::marshal( StompFrame* frame )
    throw ( MarshalException )
{
    try
    {
        CommandConstants::CommandId commandId = 
            CommandConstants::toCommandId(frame->getCommand().c_str());
        transport::Command* command = NULL;
        
        if(commandId == CommandConstants::CONNECTED){
            command = new ConnectedCommand( frame );
        }
        else if(commandId == CommandConstants::ERROR_CMD){
            command = new ErrorCommand( frame );
        }
        else if(commandId == CommandConstants::RECEIPT){
            command = new ReceiptCommand( frame );
        }
        else if(commandId == CommandConstants::MESSAGE){
            if( !frame->getProperties().hasProperty(
                    CommandConstants::toString(
                        CommandConstants::HEADER_CONTENTLENGTH ) ) )
            {
                command = new TextMessageCommand( frame );
            }
            else
            {
                command = new BytesMessageCommand( frame );
            }
        }
    
        // We either got a command or a response, but if we got neither
        // then complain, something went wrong.
        if(command == NULL)
        {
            throw MarshalException(
                __FILE__, __LINE__,
                "Marshaler::marshal - No Command Created from frame");
        }

        return command;
    }
    AMQ_CATCH_RETHROW( MarshalException )
    AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, MarshalException )
    AMQ_CATCHALL_THROW( MarshalException )
}

////////////////////////////////////////////////////////////////////////////////      
const StompFrame& Marshaler::marshal( const transport::Command* command )
    throw ( MarshalException )
{
    try
    {
        const Marshalable* marshalable = 
            dynamic_cast<const Marshalable*>(command);

        // Easy, just get the frame from the command
        if(marshalable != NULL)
        {        
            return marshalable->marshal();
        }
        else
        {
            throw MarshalException(
                __FILE__, __LINE__,
                "Marshaler::marshal - Invalid Command Type!");
        }
    }
    AMQ_CATCH_RETHROW( MarshalException )
    AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, MarshalException )
    AMQ_CATCHALL_THROW( MarshalException )
}
