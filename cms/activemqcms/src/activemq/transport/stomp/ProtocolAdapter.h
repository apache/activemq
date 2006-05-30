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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_PROTOCOLADAPTER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_PROTOCOLADAPTER_H_

#include <activemq/transport/stomp/StompMessage.h>
#include <activemq/transport/stomp/StompFrame.h>
#include <cms/Session.h>

namespace activemq{
namespace transport{
namespace stomp{
	
	// Forward declarations.
	class StompMessage;
	
    /**
     * Interface for all adapters between messages and
     * stomp frames.
     * @author Nathan Mittler
     */
	class ProtocolAdapter
	{
	public:
		virtual ~ProtocolAdapter(){};
		
		virtual StompMessage* adapt( const StompFrame* frame ) = 0;
		virtual StompFrame* adapt( const StompMessage* message ) = 0;
	
	public:
	
		static StompFrame::Command getCommandId( const StompMessage::MessageType type );
		static StompMessage::MessageType getMessageType( const StompFrame* frame );
		static cms::Session::AcknowledgeMode getAckMode( const StompFrame* frame );
		static const char* getAckModeString( const cms::Session::AcknowledgeMode mode );
		static int getAckModeStringLength( const cms::Session::AcknowledgeMode mode );
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_PROTOCOLADAPTER_H_*/
