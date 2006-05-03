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
#include "ppr/net/Socket.hpp"

using namespace apache::ppr::net;

inline int
SOCKETclose( Socket::SocketHandle sock )
{
#ifdef unix
	return ::close(sock);
#else
	return ::closesocket(sock);
#endif
}

#ifndef unix // Static socket initializer needed for winsock
Socket::StaticSocketInitializer::StaticSocketInitializer () {
    const WORD version_needed = MAKEWORD(2,2); // lo-order byte: major version
    WSAData temp;
    if (WSAStartup(version_needed, &temp)){
        socketInitError = new SocketException ("winsock.dll was not found");
    }
}
Socket::StaticSocketInitializer::~StaticSocketInitializer () {
    WSACleanup();
}

Socket::StaticSocketInitializer apache::ppr::net::Socket::staticSocketInitializer;
#endif

Socket::Socket() : socketHandle (INVALID_SOCKET_HANDLE)
{
#ifndef unix
    if (staticSocketInitializer.getSocketInitError() != NULL) {
        throw *staticSocketInitializer.getSocketInitError();
    }
#endif
}

Socket::Socket(Socket::SocketHandle socketHandle) : socketHandle (socketHandle)
{
}

Socket::~Socket()
{
    if (isConnected()) {
        // No shutdown, just close - dont want blocking destructor.
        SOCKETclose (socketHandle);
    }
}

void
Socket::connect (const char* host, int port) throw (SocketException)
{
	int ret;
    SocketHandle cs_socket_handle;
    if (isConnected()) {
        throw SocketException ("Socket already connected");
    }
	cs_socket_handle = ::socket(AF_INET, SOCK_STREAM, 0);
	if (cs_socket_handle < 0) {
        throw SocketException ("Could not create socket");
	}
    if (port <= 0 || port > 65535) {
        throw SocketException ("Port out of range");
    }
    sockaddr_in target_addr;
	target_addr.sin_family = AF_INET;
	target_addr.sin_port = htons((short)port);
	target_addr.sin_addr.s_addr = 0; // To be set later down...
    memset(&target_addr.sin_zero, 0, sizeof(target_addr.sin_zero));

    // Resolve name
    ::addrinfo hints;
    memset(&hints, 0, sizeof(addrinfo));
    hints.ai_family = PF_INET;
    struct addrinfo *res_ptr = NULL;
    if (::getaddrinfo(host, NULL, &hints, &res_ptr) != 0 || res_ptr == NULL) {
        throw SocketException ("Could not resolve name");
    } else {
        assert(res_ptr->ai_addr->sa_family == AF_INET);
        // Porting: On both 32bit and 64 bit systems that we compile to soo far, sin_addr is a 32 bit value, not an unsigned long.
        assert(sizeof(((sockaddr_in*)res_ptr->ai_addr)->sin_addr.s_addr) == 4);
        target_addr.sin_addr.s_addr = ((sockaddr_in*)res_ptr->ai_addr)->sin_addr.s_addr;
        freeaddrinfo(res_ptr);
    }

    ret = ::connect(cs_socket_handle, (const sockaddr *) &target_addr, sizeof(target_addr));
	if (ret < 0) {
        SOCKETclose (cs_socket_handle);
		throw SocketException ("Could not connect to host");
	}
    socketHandle = cs_socket_handle;
}

void
Socket::setSoTimeout (int millisecs) throw (SocketException)
{
#ifdef unix
  timeval timot;
  timot.tv_sec = millisecs / 1000;
  timot.tv_usec = (millisecs % 1000) * 1000;
#else
  int timot = millisecs;
#endif
  ::setsockopt (socketHandle, SOL_SOCKET, SO_RCVTIMEO, (const char*) &timot, sizeof (timot));
  ::setsockopt (socketHandle, SOL_SOCKET, SO_SNDTIMEO, (const char*) &timot, sizeof (timot));
}

int
Socket::getSoTimeout () const
{
#ifdef unix
  timeval timot;
  timot.tv_sec = 0;
  timot.tv_usec = 0;
  socklen_t size = sizeof(timot);
#else
  int timot = 0;
  int size = sizeof(timot);
#endif
  
  ::getsockopt (socketHandle, SOL_SOCKET, SO_RCVTIMEO, (char*) &timot, &size);
#ifdef unix
  return (timot.tv_sec * 1000) + (timot.tv_usec / 1000);
#else
  return timot;
#endif
}
#include "ppr/util/Hex.hpp"

int Socket::receive (char* buff, int size) throw (SocketException)
{
    if (!isConnected()) {
        throw SocketException ("Socket not connected");
    }
#ifdef unix
	int rc = ::read(this->socketHandle, buff, size);
    if (rc < 0) {
        throw SocketException ("Socket::receieve() failed - socket may have been disconnected") ;
    }
#else
	int rc = ::recv(this->socketHandle, buff, size, 0);
    if (rc < 0) {
        char errmsg[256];
        sprintf (errmsg, "Socket::receive() failed. WSAGetLastError() returned %d", WSAGetLastError());
        throw SocketException (errmsg);
    }
    //printf ("----receive---[%s]\n", apache::ppr::util::Hex::toString (array<char> (buff, rc))->c_str());
#endif
    return rc;
}

int
Socket::send (const char* buff, int size) throw (SocketException)
{
    if (!isConnected()) {
        throw SocketException ("Socket not connected");
    }
#ifdef unix
	int rc = ::write(this->socketHandle, buff, size);
    if (rc < 0) {
        throw SocketException ("Socket::send() failed - socket may have been disconnected");
    }
#else
	int rc = ::send(this->socketHandle, buff, size, 0);
    if (rc < 0) {
        char errmsg[256];
        sprintf (errmsg, "Socket::send() failed. WSAGetLastError() returned %d", WSAGetLastError());
        throw SocketException (errmsg);
    }
#endif
    //printf ("SOCKETSEND[%s]\n", Hex::toString (array<char> (buff, rc))->c_str());
    return rc;
}

int
Socket::send (const unsigned char* buff, int size) throw (SocketException)
{
    return send (reinterpret_cast<const char*> (buff), size);
}


void
Socket::close ()
{
    if (isConnected()) {
        ::shutdown(this->socketHandle, 2);
        SOCKETclose (this->socketHandle);
        this->socketHandle = INVALID_SOCKET_HANDLE;
    }
}

bool
Socket::isConnected() const {
    return this->socketHandle != INVALID_SOCKET_HANDLE;
}


//
// Unit test
//
#ifdef UNITTEST

#include "ppr/util/ifr/array"
#include <iostream>
#include <cassert>
#include "Thread.hpp"
#include "ServerSocket.hpp"

class TestServer : public Thread
{
public:
    TestServer () {}
protected:
    virtual void run () throw (p<exception>) 
    {
        p<ServerSocket> serverSocket = new ServerSocket();
        serverSocket->bind ("127.0.0.1", 19000);
        cout << "Server bind successful" << endl;
        p<ISocket> client = serverSocket->accept();
        cout << "Server has accepted a connection" << endl;
        array<char> buff (13);
        int count = client->receive (buff.c_array(), (int)buff.size());
        cout << "Server has received " << count << " bytes from client" << endl;
        assert (count == 13);
        assert (strcmp (buff.c_array(), "Hello Socket") == 0);
        cout << "Message was as expected" << endl;
        cout << "Server sending Good Bye to client and closing connection" << endl;
        client->send ("Good Bye", 9);
        client->close();
    }
};

void testSocket()
{
    p<TestServer> server = new TestServer();
    cout << "Starting up test server" << endl;
    server->start ();
    Socket s;
    cout << "Trying to connect to server" << endl;
    s.connect ("127.0.0.1", 19000);
    cout << "Client sending message to server" << endl;
    s.send ("Hello Socket", 13);
    cout << "Client has sent message now" << endl;
    array<char> buff (9);
    int count = s.receive (buff.c_array(), (int) buff.size());
    cout << "Client got a response from server of " << count << " bytes." << endl;
    assert (count == 9);
    assert (strcmp (buff.c_array(), "Good Bye") == 0);
    cout << "Client has verified the server's message" << endl;
    cout << "Client closes the connection" << endl;
    s.close();
}

#endif // UNITTEST

