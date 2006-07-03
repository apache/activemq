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

#ifndef _ACTIVEMQ_TRANSPORT_TCPTRANSPORT_H_
#define _ACTIVEMQ_TRANSPORT_TCPTRANSPORT_H_

#include <activemq/transport/TransportFilter.h>
#include <activemq/network/Socket.h>
#include <activemq/util/Properties.h>

namespace activemq{
namespace transport{

    /**
     * Implements a TCP/IP based transport filter, this transport
     * is meant to wrap an instance of an IOTransport.  The lower
     * level transport should take care of manaing stream reads
     * and writes.
     */
    class TcpTransport : public TransportFilter 
    {
    private:

        /**
         * Socket that this Transport Communicates with
         */
        network::Socket* socket;

    public:

    	TcpTransport( const activemq::util::Properties& properties,
                      Transport* next, 
                      const bool own = true );
    	virtual ~TcpTransport(void);

    };

}}

#endif /*_ACTIVEMQ_TRANSPORT_TCPTRANSPORT_H_*/
