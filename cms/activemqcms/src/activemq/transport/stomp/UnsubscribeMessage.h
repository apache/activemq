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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_UNSUBSCRIBEMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_UNSUBSCRIBEMESSAGE_H_

#include <activemq/transport/stomp/DestinationMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Message sent to the broker to unsubscribe to a
     * topic or queue.
     * @author Nathan Mittler
     */
	class UnsubscribeMessage : public DestinationMessage
	{
	public:
		
		virtual ~UnsubscribeMessage(){};
		
		virtual MessageType getMessageType() const{
			return MSG_UNSUBSCRIBE;
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
		
		virtual cms::Message* clone() const{
			UnsubscribeMessage* msg = new UnsubscribeMessage();
			msg->destination = destination;
			return msg->getCMSMessage();
		}
		
	private:
	
		std::string destination;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_UNSUBSCRIBEMESSAGE_H_*/
