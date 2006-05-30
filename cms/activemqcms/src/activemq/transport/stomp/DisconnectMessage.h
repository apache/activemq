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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_DISCONNECTMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_DISCONNECTMESSAGE_H_

#include <activemq/transport/stomp/StompMessage.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Sent to the broker to disconnect gracefully before closing
     * the socket.
     * @author Nathan Mittler
     */
	class DisconnectMessage : public StompMessage
	{
	public:
		
		virtual ~DisconnectMessage(){};
		
		virtual MessageType getMessageType() const{
			return MSG_DISCONNECT;
		}
		
		virtual const cms::Message* getCMSMessage() const{
			return NULL;
		}
		
		virtual cms::Message* getCMSMessage(){
			return NULL;
		}
		
		virtual cms::Message* clone() const{
			DisconnectMessage* msg = new DisconnectMessage();
			return msg->getCMSMessage();
		}
		
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_DISCONNECTMESSAGE_H_*/


