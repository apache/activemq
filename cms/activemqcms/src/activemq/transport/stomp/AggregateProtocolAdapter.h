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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_AGGREGATEPROTOCOLADAPTER_H_
#define ACTIVEMQ_TRANSPORT_STOMP_AGGREGATEPROTOCOLADAPTER_H_
 
#include <activemq/transport/stomp/ProtocolAdapter.h>
#include <vector>

namespace activemq{
namespace transport{
namespace stomp{
	
    /**
     * A protocol adapter that contains all adapters for
     * the stomp protocol.
     * @author Nathan Mittler
     */
	class AggregateProtocolAdapter : public ProtocolAdapter
	{
	public:
		AggregateProtocolAdapter();
		virtual ~AggregateProtocolAdapter();
		
		virtual StompMessage* adapt( const StompFrame* frame );
		virtual StompFrame* adapt( const StompMessage* message );
		
	private:
	
		std::vector<ProtocolAdapter*> adapters;
	};

}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_AGGREGATEPROTOCOLADAPTER_H_*/
