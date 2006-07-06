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

#ifdef unix
#include <unistd.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#else
#include <Winsock2.h>
#include <Ws2tcpip.h> 
#include <sys/stat.h>
#define stat _stat
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <sys/types.h>
#include "ppr/net/ServerSocket.hpp"

using namespace apache::ppr::net;

inline int SOCKETclose( Socket::SocketHandle sock )
{
#ifdef unix
	return ::close(sock);
#else
	return ::closesocket(sock);
#endif
}

ServerSocket::ServerSocket() : socketHandle (Socket::INVALID_SOCKET_HANDLE)
{
#ifndef unix
    if( Socket::staticSocketInitializer.getSocketInitError() != NULL )
        throw *Socket::staticSocketInitializer.getSocketInitError();
#endif
}

ServerSocket::~ServerSocket()
{
    if( isBound() )
    {
        // No shutdown, just close - dont want blocking destructor.
        SOCKETclose (socketHandle);
    }
}

void ServerSocket::bind (const char* host, int port) throw (SocketException)
{
    bind(host, port, SOMAXCONN);
}

void ServerSocket::bind (const char* host, int port, int backlog) throw (SocketException)
{
    SocketHandle listen_socket_handle;
	int          ret ;

    if( isBound() )
        throw SocketException ("Socket already bound") ;

	listen_socket_handle = ::socket(AF_INET, SOCK_STREAM, 0) ;

    if( listen_socket_handle < 0 )
        throw SocketException ("Could not create socket");

    if( port <= 0 || port > 65535 )
        throw SocketException ("Port out of range");

    sockaddr_in bind_addr;
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_port = htons((short)port);
	bind_addr.sin_addr.s_addr = 0; // To be set later down...
    memset(&bind_addr.sin_zero, 0, sizeof(bind_addr.sin_zero));

    // Resolve name
    ::addrinfo hints;
    memset(&hints, 0, sizeof(addrinfo));
    hints.ai_family = PF_INET;
    struct addrinfo *res_ptr = NULL;

    if( ::getaddrinfo(host, NULL, &hints, &res_ptr) != 0 || res_ptr == NULL )
        throw SocketException ("Could not resolve name");
    else
    {
        assert(res_ptr->ai_addr->sa_family == AF_INET);
        // Porting: On both 32bit and 64 bit systems that we compile to soo far, sin_addr is a 32 bit value, not an unsigned long.
        assert(sizeof(((sockaddr_in*)res_ptr->ai_addr)->sin_addr.s_addr) == 4);
        bind_addr.sin_addr.s_addr = ((sockaddr_in*)res_ptr->ai_addr)->sin_addr.s_addr;
        freeaddrinfo(res_ptr);
    }

	ret = ::bind(listen_socket_handle,
				 (sockaddr *)&bind_addr, sizeof(bind_addr));

	if( ret < 0 )
    {
        SOCKETclose (listen_socket_handle);
		throw SocketException ("Could not bind");
	}
    ret = ::listen(listen_socket_handle, backlog);
    if( ret < 0 )
    {
        SOCKETclose (listen_socket_handle);
        throw SocketException ("Could not listen");
    }
    socketHandle = listen_socket_handle;
}

void ServerSocket::close ()
{
    if( isBound() )
    {
        SOCKETclose (this->socketHandle);
        this->socketHandle = Socket::INVALID_SOCKET_HANDLE;
    }
}

bool ServerSocket::isBound() const
{
    return this->socketHandle != Socket::INVALID_SOCKET_HANDLE;
}

p<ISocket> ServerSocket::accept () throw (SocketException)
{
    struct sockaddr_in temp;
#ifdef unix
    socklen_t temp_len = sizeof (sockaddr_in);
#else
    int temp_len = sizeof (sockaddr_in);
#endif
    SocketHandle ss_socket_handle = Socket::INVALID_SOCKET_HANDLE;

    ss_socket_handle = ::accept(this->socketHandle, (struct sockaddr*)&temp, &temp_len);
    if (ss_socket_handle < 0) {
        throw SocketException ("Accept Error");
    }
    return new Socket (ss_socket_handle);
}
