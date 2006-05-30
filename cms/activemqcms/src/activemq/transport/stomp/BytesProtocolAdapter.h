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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_BYTESPROTOCOLADAPTER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_BYTESPROTOCOLADAPTER_H_
 
#include <activemq/transport/stomp/ProtocolAdapter.h>
#include <activemq/transport/stomp/StompBytesMessage.h>

namespace activemq{
namespace transport{
namespace stomp{

    /**
     * Adapts between bytes messages and stomp frames.
     * @author Nathan Mittler
     */
	class BytesProtocolAdapter : public ProtocolAdapter{
	public:
	
		virtual ~BytesProtocolAdapter(){}
		
		virtual StompMessage* adapt( const StompFrame* frame ){
			const StompFrame::HeaderInfo* dest = frame->getHeaderInfo( StompFrame::HEADER_DESTINATION );
			const StompFrame::HeaderInfo* transaction = frame->getHeaderInfo( StompFrame::HEADER_TRANSACTIONID );
						
			StompBytesMessage* msg = new StompBytesMessage();
			msg->setDestination( dest->value );
			msg->setData( frame->getBody(), frame->getBodyLength() );
			if( transaction != NULL ){
				msg->setTransactionId( transaction->value );
			}
			
			return (DestinationMessage*)msg;
		}
		
		virtual StompFrame* adapt( const StompMessage* message ){			
			StompFrame* frame = new StompFrame();
			
			const StompBytesMessage* msg = dynamic_cast<const StompBytesMessage*>(message);
			
			// Set the command.
			frame->setCommand( getCommandId( msg->getMessageType() ) );
			
			// Set the destination.
			frame->setHeader( StompFrame::HEADER_DESTINATION, 
				msg->getDestination(),
				strlen( msg->getDestination() ) );
			
			// Set transaction info (if available).
			if( msg->isTransaction() ){
				frame->setHeader( StompFrame::HEADER_TRANSACTIONID, 
					msg->getTransactionId(),
					strlen( msg->getTransactionId() ) );
			}
			
			// Set the body data.
			frame->setBodyBytes( msg->getData(), msg->getNumBytes() );
			
			return frame;
		}
	};
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_BYTESPROTOCOLADAPTER_H_*/
