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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_SUBSCRIBEMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_SUBSCRIBEMESSAGE_H_

#include <activemq/transport/stomp/DestinationMessage.h>
#include <cms/Session.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Message sent to the broker to subscribe to a topic
     * or queue.
     * @author Nathan Mittler
     */
	class SubscribeMessage : public DestinationMessage
	{
	public:
		
		SubscribeMessage(){
			mode = cms::Session::AUTO_ACKNOWLEDGE;
		}
		virtual ~SubscribeMessage(){};		
		
		virtual MessageType getMessageType() const{
			return MSG_SUBSCRIBE;
		}
		
		virtual const cms::Message* getCMSMessage() const{
			return NULL;
		}
		
		virtual cms::Message* getCMSMessage(){
			return NULL;
		}
		
		virtual void setDestination( const char* destination ){
			this->destination = destination;
		}
		
		virtual const char* getDestination() const{
			return destination.c_str();
		}
		
		virtual void setAckMode( const cms::Session::AcknowledgeMode mode ){
			this->mode = mode;
		}
		
		virtual cms::Session::AcknowledgeMode getAckMode() const{
			return mode;
		}
		
		virtual cms::Message* clone() const{
			SubscribeMessage* msg = new SubscribeMessage();
			msg->destination = destination;
			msg->mode = mode;
			return msg->getCMSMessage();
		}
		
	public:
	
		std::string destination;
		cms::Session::AcknowledgeMode mode;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_SUBSCRIBEMESSAGE_H_*/
