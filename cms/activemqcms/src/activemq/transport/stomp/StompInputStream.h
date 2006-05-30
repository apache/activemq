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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPINPUTSTREAM_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPINPUTSTREAM_H_
 
#include <activemq/io/InputStream.h>

namespace activemq{
namespace transport{
namespace stomp{
	
	// Forward declarations.
	class StompFrame;
	
    /**
     * Stream that can read stomp frames.
     * @author Nathan Mittler
     */
	class StompInputStream : public io::InputStream{
	public:
	
		virtual ~StompInputStream(){};
		
		virtual StompFrame* readStompFrame() throw (ActiveMQException) = 0;
	};
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPINPUTSTREAM_H_*/
