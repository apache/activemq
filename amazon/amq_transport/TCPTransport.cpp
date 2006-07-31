/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

#include <string>

#include "TransportFactory.h"
#include "TransportInitializer.h"
#include "TCPTransport.h"

#include "amq_corelib/Exception.h"
#include "amq_corelib/RCSID.h"

using ActiveMQ::TCPTransport;
using ActiveMQ::Transport;
using ActiveMQ::TransportInitializer;
using ActiveMQ::Exception;
using std::string;
using std::auto_ptr;

RCSID(TCPTransport, "$Id$");

void
TCPTransport::connect() {
    if (!connected_) {

        struct addrinfo hints, *res;

        memset(&hints, '\0', sizeof(struct addrinfo));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        int rc = getaddrinfo(host_.c_str(), port_.c_str(), &hints, &res);
        if (rc != 0)
            throw Exception(gai_strerror(rc));

        rc = ::connect(fd_, res->ai_addr, res->ai_addrlen);
        freeaddrinfo(res);
        if (rc == -1)
            throw Exception(errno >= sys_nerr
                            ? "Unknown error"
                            : sys_errlist[errno]);

        connected_ = true;
    }
}

TCPTransport::TCPTransport(const string& host, const string& port) :
    host_(host), port_(port), connected_(false) {
    
    fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_ == -1)
        throw Exception(errno >= sys_nerr
                        ? "Unknown error"
                        : sys_errlist[errno]);
}

int
TCPTransport::send(const unsigned char *buf, size_t count) {
    if (!connected_)
        throw Exception("send called on unconnected transport");
    if (buf == NULL)
        throw Exception("NULL buffer passed to send");
    int rc = ::send(fd_, (void *)buf, count, 0);
    if (rc == -1) {
        connected_ = false;
        throw Exception(errno >= sys_nerr
                        ? "Unknown error"
                        : sys_errlist[errno]);
    }
    return rc;
}

int
TCPTransport::recv(unsigned char *buf, size_t count) {
    if (!connected_)
        throw Exception("recv called on unconnected transport");
    if (buf == NULL)
        throw Exception("NULL buffer passed to recv");
    int rc = ::recv(fd_, (void *)buf, count, 0);
    if (rc == -1) {
        connected_ = false;
        throw Exception(errno >= sys_nerr
                        ? "Unknown error"
                        : sys_errlist[errno]);
    }
    return rc;
}

void
TCPTransport::disconnect() {
    if (connected_) {
        connected_ = false;
        int rc = ::close(fd_);
        if (rc == -1)
            throw Exception(errno >= sys_nerr
                            ? "Unknown error"
                            : sys_errlist[errno]);
    }
}

TCPTransport::~TCPTransport() {
    try {
        disconnect();
    } catch (...) {}
}

bool
TCPTransport::isConnected() const {
    return connected_;
}

TCPTransport::TCPTransport(const string& uri)
    : connected_(false)
{
    string::size_type colpos = uri.find(":");
    string hostport(uri, colpos + 3, uri.size() - colpos - 3);

    host_.assign(hostport, 0, hostport.find(":"));
    port_.assign(hostport, hostport.find(":") + 1,
                hostport.size() - hostport.find(":"));
    
    fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_ == -1)
        throw Exception(errno >= sys_nerr
                        ? "Unknown error"
                        : sys_errlist[errno]);
}    

auto_ptr<Transport>
initFromURI_(const string& uri) {
    return auto_ptr<Transport>(new TCPTransport(uri));
}

int
TCPTransport::dummy() {
    return 1;
}

static TransportInitializer init =
    TransportInitializer("tcp", initFromURI_);
