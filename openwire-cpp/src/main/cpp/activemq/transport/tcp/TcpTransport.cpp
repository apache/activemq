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
#include "activemq/transport/tcp/TcpTransport.hpp"

using namespace std;
using namespace apache::activemq::transport::tcp;


// --- Constructors -------------------------------------------------

/*
 *
 */
TcpTransport::TcpTransport(p<ISocket> socket, p<IProtocol> wireProtocol)
{
    // Initialize members
    this->socket     = socket ;
    this->protocol   = wireProtocol ;
    this->istream    = NULL ;
    this->ostream    = NULL ;
    this->listener   = NULL ;
    this->readThread = NULL ;
    this->started    = false ;
    this->closed     = false ;
}

/*
 *
 */
TcpTransport::~TcpTransport()
{
    closed = true ;
    readThread->join() ;
    istream->close() ;
    ostream->close() ;
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
void TcpTransport::setCommandListener(p<ICommandListener> listener)
{
    this->listener = listener ;
}

/*
 *
 */
p<ICommandListener> TcpTransport::getCommandListener()
{
    return this->listener ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
void TcpTransport::start()
{
    if( !started )
    {
        // Must have a command listener
        if( listener == NULL )
			throw InvalidOperationException("Command listener cannot be null when TCP transport start is called.") ;

        started = true ;

        // Create the I/O streams
        ostream = new DataOutputStream( new BufferedOutputStream( new SocketOutputStream(socket) ) ) ;
        istream = new DataInputStream( new BufferedInputStream( new SocketInputStream(socket) ) ) ;
        
        // Create and start the background read thread
        readThread = new ReadThread(this) ;
        readThread->start() ;

        // Ask protocol handler to handshake
        protocol->handshake( smartify(this) ) ;
    }
}

/*
 *
 */
void TcpTransport::oneway(p<BaseCommand> command)
{
    protocol->marshal(command, ostream) ;
    ostream->flush() ;
}

/*
 *
 */
p<FutureResponse> TcpTransport::asyncRequest(p<BaseCommand> command)
{
    throw InvalidOperationException("Use a CorrelatorFilter if you want to issue asynchrounous request calls.") ;
}

/*
 *
 */
p<Response> TcpTransport::request(p<BaseCommand> command)
{
    throw InvalidOperationException("Use a CorrelatorFilter if you want to issue request calls.") ;
}


// --- Implementation methods ---------------------------------------

/*
 *
 */
void TcpTransport::readLoop()
{
    // Continue loop until closed or aborted
    while( !closed )
    {
        p<BaseCommand> command = NULL ;

        try
        {
            // Read next command
            command = p_cast<BaseCommand> (protocol->unmarshal(istream)) ;

            // Forward to command listener

            listener->onCommand(smartify(this), command) ;
        }
        catch( exception& e )
        {
            // Socket closed or error
            if( !closed )
                listener->onError(smartify(this), e) ;

            cout << "Exiting read loop due to exception: " << e.what() << endl ;
            break ;
        }
    }
}
