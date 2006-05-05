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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPMESSAGE_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPMESSAGE_H_

#include <cms/Message.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * Interface for all stomp messages received from the
     * broker.
     * @author Nathan Mittler
     */
	class StompMessage{
	public:
	
		enum MessageType{
			MSG_CONNECT,
			MSG_CONNECTED,
			MSG_DISCONNECT,
			MSG_SUBSCRIBE,
			MSG_UNSUBSCRIBE,
			MSG_TEXT,
			MSG_BYTES,
			MSG_BEGIN,
			MSG_COMMIT,
			MSG_ABORT,
			MSG_ACK,
			MSG_ERROR,
			NUM_MSG_TYPES
		};
		
	public:
	
		virtual ~StompMessage(){}
		
		virtual MessageType getMessageType() const = 0;
		virtual const cms::Message* getCMSMessage() const = 0;
		virtual cms::Message* getCMSMessage() = 0;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPMESSAGE_H_*/
