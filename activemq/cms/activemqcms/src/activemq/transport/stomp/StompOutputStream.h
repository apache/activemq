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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPOUTPUTSTREAM_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPOUTPUTSTREAM_H_
 
#include <activemq/io/OutputStream.h>
#include <activemq/transport/stomp/StompFrame.h>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * An output stream for stomp frames.
     * @author Nathan Mittler
     */
	class StompOutputStream : public io::OutputStream{
	public:
	
		virtual ~StompOutputStream(){}
		
		virtual void writeStompFrame( StompFrame& frame ) throw( ActiveMQException ) = 0;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPOUTPUTSTREAM_H_*/
