/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "activemq/transport/TransportFactory.hpp"
#include <cctype>
#include <algorithm>

using namespace apache::activemq::transport;


// --- Constructors -------------------------------------------------

/*
 *
 */
TransportFactory::TransportFactory()
{
    socketFactory = new SocketFactory() ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
p<ITransport> TransportFactory::createTransport(p<Uri> location) throw (SocketException, IllegalArgumentException)
{
    p<ISocket>    socket ;
    p<ITransport> transport ;
    p<IProtocol>  protocol ;
    string        uriString ;

    // Make an URI all lower case string
    uriString = location->toString() ;
    std::transform(uriString.begin(), uriString.end(), uriString.begin(), (int(*)(int))tolower) ;  // The explicit cast is needed to compile on Linux

    // Create and open socket
    cout << "Opening socket to: " << location->host() << " on port " << location->port() << endl ;
    socket = connect(location->host().c_str(), location->port()) ;

    // Create wire protocol depending on specified query parameter
    if( uriString.find("protocol=stomp") != string::npos )
        throw IllegalArgumentException("The STOMP protocol is not yet implemented") ;
    else
        protocol = new OpenWireProtocol() ;

    // Configure character encoding depending on specified query parameter
    if( uriString.find("encoding=none") != string::npos )
        CharsetEncoderRegistry::DEFAULT = NULL ;
    else
        CharsetEncoderRegistry::DEFAULT = AsciiToUTF8Encoder::NAME ;

    // Create transport depending on specified URI scheme
    if( uriString.find("tcp://") != string::npos )
	    transport = new TcpTransport(socket, protocol) ;
    else
        throw IllegalArgumentException("Cannot create transport for unknown URI scheme") ;

    // Chain logging filter is requested in URI query
    if( uriString.find("trace=true") != string::npos )
        transport = new LoggingFilter(transport) ;

    // Chain correlator and mutext filters
	transport = new CorrelatorFilter(transport) ;
	transport = new MutexFilter(transport) ;

	return transport ;
}


// --- Implementation methods ---------------------------------------

/*
 *
 */
p<ISocket> TransportFactory::connect(const char* host, int port) throw (SocketException)
{
    p<ISocket> socket = socketFactory->createSocket() ;

 	// Try to connect socket to given address and port
    socket->connect(host, port) ;
    return socket ;
}
