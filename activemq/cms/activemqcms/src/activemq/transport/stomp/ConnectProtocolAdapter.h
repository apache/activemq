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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_CONNECTPROTOCOLADAPTER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_CONNECTPROTOCOLADAPTER_H_

#include <activemq/transport/stomp/ProtocolAdapter.h>
#include <activemq/transport/stomp/ConnectMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Adapts between connect messages and stomp frames.
     * @author Nathan Mittler
     */
	class ConnectProtocolAdapter : public ProtocolAdapter
	{
	public:
	
		virtual ~ConnectProtocolAdapter(){};
		
		virtual StompMessage* adapt( const StompFrame* frame ){
			const StompFrame::HeaderInfo* login = frame->getHeaderInfo( StompFrame::HEADER_LOGIN );
			const StompFrame::HeaderInfo* password = frame->getHeaderInfo( StompFrame::HEADER_PASSWORD );
			
			ConnectMessage* msg = new ConnectMessage();
			msg->setLogin( login->value );
			msg->setPassword( password->value );
			return msg;
		}
		
		virtual StompFrame* adapt( const StompMessage* message ){			
			StompFrame* frame = new StompFrame();
		
			const ConnectMessage* connectMsg = dynamic_cast<const ConnectMessage*>(message);
			
			// Set the command.
			frame->setCommand( getCommandId( connectMsg->getMessageType() ) );

			// Set the login.
			frame->setHeader( StompFrame::HEADER_LOGIN, 
				connectMsg->getLogin().c_str(),
				connectMsg->getLogin().size() );
			
			// Set the password.
			frame->setHeader( StompFrame::HEADER_PASSWORD, 
				connectMsg->getPassword().c_str(),
				connectMsg->getPassword().size() );
			
			return frame;
		}
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_CONNECTPROTOCOLADAPTER_H_*/
