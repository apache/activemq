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
#include <activemq/network/SocketFactory.h>
#include <activemq/network/BufferedSocket.h>
#include <activemq/network/TcpSocket.h>
#include <activemq/util/Properties.h>

using namespace std;
using namespace activemq;
using namespace activemq::util;
using namespace activemq::network;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
Socket* SocketFactory::createSocket(const Properties& properties)
    throw ( SocketException )
{
    try
    {
        const char* uri = properties.getProperty( "uri" );
        if( uri == NULL )
        {
            throw SocketException( __FILE__, __LINE__, 
                "SocketTransport::start() - uri not provided" );
        }

        string dummy = uri;
      
        // Extract the port.
        unsigned int portIx = dummy.find( ':' );
        if( portIx == string::npos )
        {
            throw SocketException( __FILE__, __LINE__, 
                "SocketTransport::start() - uri malformed - port not specified: %s", uri);
        }
        string host = dummy.substr( 0, portIx );
        string portString = dummy.substr( portIx + 1 );
        int port;
        if( sscanf( portString.c_str(), "%d", &port) != 1 )
        {
            throw SocketException( __FILE__, __LINE__, 
               "SocketTransport::start() - unable to extract port from uri: %s", uri);
        }
      
        // Get the read buffer size.
        int inputBufferSize = 10000;
        dummy = properties.getProperty( "inputBufferSize", "10000" );  
        sscanf( dummy.c_str(), "%d", &inputBufferSize );
      
        // Get the write buffer size.
        int outputBufferSize = 10000;
        dummy = properties.getProperty( "outputBufferSize", "10000" ); 
        sscanf( dummy.c_str(), "%d", &outputBufferSize );
      
        // Get the linger flag.
        int soLinger = 0;
        dummy = properties.getProperty( "soLinger", "0" ); 
        sscanf( dummy.c_str(), "%d", &soLinger ); 
      
        // Get the keepAlive flag.
        bool soKeepAlive = 
            properties.getProperty( "soKeepAlive", "false" ) == "true";   
      
        // Get the socket receive buffer size.
        int soReceiveBufferSize = 2000000;
        dummy = properties.getProperty( "soReceiveBufferSize", "2000000" );  
        sscanf( dummy.c_str(), "%d", &soReceiveBufferSize );
      
        // Get the socket send buffer size.
        int soSendBufferSize = 2000000;
        dummy = properties.getProperty( "soSendBufferSize", "2000000" );  
        sscanf( dummy.c_str(), "%d", &soSendBufferSize );
      
        // Get the socket send buffer size.
        int soTimeout = 10000;
        dummy = properties.getProperty( "soTimeout", "10000" );  
        sscanf( dummy.c_str(), "%d", &soTimeout );
      
        // Now that we have all the elements that we wanted - let's do it!
        // Create a TCP Socket and then Wrap it in a buffered socket
        // so that users get the benefit of buffered reads and writes.
        // The buffered socket will own the TcpSocket instance, and will
        // clean it up when it is cleaned up.
        TcpSocket* tcpSocket = new TcpSocket();
        BufferedSocket* socket = 
            new BufferedSocket(tcpSocket, inputBufferSize, outputBufferSize);
      
        // Connect the socket.
        socket->connect( host.c_str(), port );
      
        // Set the socket options.
        socket->setSoLinger( soLinger );
        socket->setKeepAlive( soKeepAlive );
        socket->setReceiveBufferSize( soReceiveBufferSize );
        socket->setSendBufferSize( soSendBufferSize );
        socket->setSoTimeout( soTimeout );

        return socket;
    }
    AMQ_CATCH_RETHROW( SocketException )
    AMQ_CATCH_EXCEPTION_CONVERT( ActiveMQException, SocketException )
    AMQ_CATCHALL_THROW( SocketException )   
}
