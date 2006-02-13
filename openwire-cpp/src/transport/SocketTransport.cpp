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
#include <iostream>
#include <exception>
#include <typeinfo>
#include "transport/SocketTransport.hpp"

using namespace std;
using namespace apache::activemq::client::transport;


// --- Constructors -------------------------------------------------

/*
 *
 */
SocketTransport::SocketTransport(const char* host, int port)
{
	// Initialize APR framework
    init() ;

    cout << "Opening socket to: " << host << " on port: " << port << endl ;

    // Open socket
    socket = connect(host, port) ;
    writer = new SocketBinaryWriter(socket) ;
    reader = new SocketBinaryReader(socket) ;
}

/*
 *
 */
SocketTransport::~SocketTransport()
{
	// Destroy memory pools (including sub pool) and terminate APR
	if( memoryPool != NULL )
		apr_pool_destroy(memoryPool) ;

	// Terminate APR framework
	apr_terminate() ;
}


// --- Attribute methods --------------------------------------------


// --- Operation methods --------------------------------------------

/*
 *
 */
void SocketTransport::oneway(p<ICommand> command)
{
    p<BaseCommand> baseCommand = (p<BaseCommand>&)command ;

    // Set command id and that no response is required
    baseCommand->setCommandId( getNextCommandId() ) ;
    baseCommand->setResponseRequired(false) ;

    send(command) ;
}

/*
 *
 */
p<FutureResponse> SocketTransport::asyncRequest(p<ICommand> command)
{
    p<BaseCommand> baseCommand = (p<BaseCommand>&)command ;

    // Set command id and that a response is required
    baseCommand->setCommandId( getNextCommandId() ) ;
    baseCommand->setResponseRequired(true) ;
    send(command) ;

    // Register a future response holder with the command id
    p<FutureResponse> future = new FutureResponse() ;
    requestMap[baseCommand->getCommandId()] = future ;

    return future ;
}

/*
 *
 */
p<Response> SocketTransport::request(p<ICommand> command)
{
    p<FutureResponse> future = asyncRequest(command) ;
    return future->getResponse() ;
}


// --- Implementation methods ---------------------------------------

/*
 *
 */
void SocketTransport::init()
{
    apr_threadattr_t *attribs ;
    apr_status_t     rc ;

	// Initialize APR framework
    rc = apr_initialize() ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to initialize APR framework.") ;
   
    // Create APR memory pool
    rc = apr_pool_create(&memoryPool, NULL) ;
    if( rc != APR_SUCCESS )
	    throw exception("Failed to allocate APR memory pool.") ;

    // Create transmission mutex
    apr_thread_mutex_create(&mutex, APR_THREAD_MUTEX_UNNESTED, memoryPool) ;

	// Create attached thread attribute so main thread can terminate it
    apr_threadattr_create(&attribs, memoryPool) ;
    apr_threadattr_detach_set(attribs, 0) ;

    // Create and start the background read thread
    apr_thread_create(&readThread, attribs, this->readLoop, this, memoryPool) ;
}

/*
 *
 */
void SocketTransport::acquireLock()
{
    apr_thread_mutex_lock(mutex) ;
}

/*
 *
 */
void SocketTransport::releaseLock()
{
    apr_thread_mutex_unlock(mutex) ;
}

/*
 *
 */
void SocketTransport::send(p<ICommand> command)
{
    // Wait for lock and then transmit command
    acquireLock() ;
    CommandMarshallerRegistry::writeCommand(command, writer) ;
    releaseLock() ;
}

/*
 *
 */
long SocketTransport::getNextCommandId()
{
    long id ;

    // Wait for lock and then fetch next command id
    acquireLock() ;
    id = ++nextCommandId ;
    releaseLock() ;

    return id ;
}

/*
 *
 */
apr_socket_t* SocketTransport::connect(const char* host, int port)
{
    apr_socket_t* sock ;
	apr_status_t   rc ;
    
	// Look up the remote address
	rc = apr_sockaddr_info_get(&remote_sa, host, APR_UNSPEC, port, 0, memoryPool) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to lookup remote address") ;
	
	// Create socket
	rc = apr_socket_create(&sock, remote_sa->sa.sin.sin_family, SOCK_STREAM, APR_PROTO_TCP, memoryPool) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to create socket") ;

	// Connect socket
    rc = apr_socket_connect(sock, remote_sa) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to connect socket") ;
   
    // Get socket info
    rc = apr_socket_addr_get(&remote_sa, APR_REMOTE, sock) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to fetch remote socket address") ;
    rc = apr_sockaddr_ip_get(&remote_ip, remote_sa) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to fetch remote IP address") ;
    rc = apr_socket_addr_get(&local_sa, APR_LOCAL, sock) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to fetch local socket address") ;
    rc = apr_sockaddr_ip_get(&local_ip, local_sa) ;
    if( rc != APR_SUCCESS )
        throw exception("Failed to fetch local IP address") ;
   
	return sock ;
}

/*
 *
 */
void* APR_THREAD_FUNC SocketTransport::readLoop(apr_thread_t* thread, void* data)
{
    SocketTransport *transport = (SocketTransport*)data ;

    cout << "Starting to read commands from ActiveMQ" << endl ;

    while( !transport->closed )
    {
        p<BaseCommand> baseCommand = NULL ;

        try
        {
            baseCommand = (p<BaseCommand>&)CommandMarshallerRegistry::readCommand(transport->reader) ;
        }
        catch( exception e )
        {
            // socket closed
            break ;
        }
        if( typeid(baseCommand) == typeid(Response) )
        {
            p<Response>       response = (p<Response>&)baseCommand ;
            p<FutureResponse> future = (p<FutureResponse>&)transport->requestMap[response->getCommandId()] ;

            cout << "Received response: " << response->getCommandTypeAsString( response->getCommandType() )->c_str() << endl ;

            if( future != NULL )
            {
                if( typeid(response) == typeid(ExceptionResponse) )
                {
                    p<ExceptionResponse> er  = (p<ExceptionResponse>&)response ;
                    p<OpenWireException> owe = new BrokerException( er->getException() ) ;
                    // TODO: Forward incoming exception
                }
                else
                {
                    future->setResponse(response) ;
                }
            }
            else
            {
                cout << "Unknown response ID: " << response->getCommandId() << endl ;
            }
        }
        else
        {
            // TODO: Forward incoming response
        }
    }
    return NULL  ;
}
