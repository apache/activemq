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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_ERRORPROTOCOLADAPTER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_ERRORPROTOCOLADAPTER_H_

#include <activemq/transport/stomp/ProtocolAdapter.h>
#include <activemq/transport/stomp/ErrorMessage.h>

namespace activemq{
namespace transport{
namespace stomp{

    /**
     * Adapts between error messages and stomp frames.
     * @author Nathan Mittler
     */
	class ErrorProtocolAdapter : public ProtocolAdapter{
	public:
	
		virtual ~ErrorProtocolAdapter(){}
		
		virtual StompMessage* adapt( const StompFrame* frame ){
			const StompFrame::HeaderInfo* title = frame->getHeaderInfo( StompFrame::HEADER_MESSAGE );
						
			ErrorMessage* msg = new ErrorMessage();
			msg->setErrorTitle( title->value );
			msg->setErrorText( frame->getBody() );
			
			return msg;
		}
		
		virtual StompFrame* adapt( const StompMessage* message ){			
			StompFrame* frame = new StompFrame();
			
			const ErrorMessage* msg = dynamic_cast<const ErrorMessage*>(message);
			
			// Set command.
			frame->setCommand( getCommandId( msg->getMessageType() ) );
			
			// Set the "message" header
			frame->setHeader( StompFrame::HEADER_MESSAGE, 
				msg->getErrorTitle().c_str(),
				msg->getErrorTitle().size() );
			
			// Set the error text.
			frame->setBodyText( msg->getErrorText().c_str(),
				msg->getErrorText().size() );
			
			return frame;
		}
	};
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_ERRORPROTOCOLADAPTER_H_*/
