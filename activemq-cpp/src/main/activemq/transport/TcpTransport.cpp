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

#include "TcpTransport.h"

#include <activemq/network/SocketFactory.h>
#include <activemq/transport/IOTransport.h>
#include <activemq/transport/TransportFactory.h>
#include <activemq/transport/TransportFactoryMap.h>

using namespace std;
using namespace activemq;
using namespace activemq::transport;
using namespace activemq::network;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
TcpTransport::TcpTransport( const activemq::util::Properties& properties,
                            Transport* next, 
                            const bool own )
 : TransportFilter( next, own )
{
    try
    {
        // Create the IO device we will be communicating over the
        // wire with.  This may need to change if we add more types
        // of sockets, such as SSL.  
        socket = SocketFactory::createSocket( properties );

        // Cast it to an IO transport so we can wire up the socket
        // input and output streams.
        IOTransport* ioTransport = dynamic_cast<IOTransport*>( next );
        if( ioTransport == NULL ){
            throw ActiveMQException( 
                __FILE__, __LINE__, 
                "TcpTransport::TcpTransport - "
                "transport must be of type IOTransport");
        }

        // Give the IOTransport the streams from out TCP socket.        
        ioTransport->setInputStream( socket->getInputStream() );
        ioTransport->setOutputStream( socket->getOutputStream() );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
TcpTransport::~TcpTransport(void)
{
    try
    {
        socket->close();
        delete socket;
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

