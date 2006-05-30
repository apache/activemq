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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_TEXTPROTOCOLADAPTER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_TEXTPROTOCOLADAPTER_H_

#include <activemq/transport/stomp/ProtocolAdapter.h>
#include <activemq/transport/stomp/StompTextMessage.h>

namespace activemq{
namespace transport{
namespace stomp{

    /**
     * Adapts between text messages and stomp frames.
     * @author Nathan Mittler
     */
	class TextProtocolAdapter : public ProtocolAdapter{
	public:
	
		virtual ~TextProtocolAdapter(){}
		
		virtual StompMessage* adapt( const StompFrame* frame ){
			const StompFrame::HeaderInfo* dest = frame->getHeaderInfo( StompFrame::HEADER_DESTINATION );
			const StompFrame::HeaderInfo* transaction = frame->getHeaderInfo( StompFrame::HEADER_TRANSACTIONID );
			
			StompTextMessage* msg = new StompTextMessage();
			msg->setDestination( dest->value );
			msg->setText( frame->getBody() );
			if( transaction != NULL ){
				msg->setTransactionId( transaction->value );
			}
			
			return (DestinationMessage*)msg;
		}
		virtual StompFrame* adapt( const StompMessage* message ){			
			StompFrame* frame = new StompFrame();
			
			const StompTextMessage* msg = dynamic_cast<const StompTextMessage*>(message);
			
			// Set the command.
			frame->setCommand( getCommandId( msg->getMessageType() ) );
			
			// Set the destination header.
			frame->setHeader( StompFrame::HEADER_DESTINATION, 
				msg->getDestination(),
				strlen( msg->getDestination() ) );
			
			// Set the transaction header (if provided)
			if( msg->isTransaction() ){
				frame->setHeader( StompFrame::HEADER_TRANSACTIONID, 
				msg->getTransactionId(),
				strlen( msg->getTransactionId() ) );
			}
			
			// Set the text.
			frame->setBodyText( msg->getText(), strlen( msg->getText() ) );
			
			return frame;
		}
	};
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_TEXTPROTOCOLADAPTER_H_*/
