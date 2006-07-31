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

#ifndef ACTIVEMQ_TCP_TRANSPORT_H
#define ACTIVEMQ_TCP_TRANSPORT_H

#include <string>
#include <memory>

#include "Transport.h"

namespace ActiveMQ {
    /// Represents a transport over a TCP connection.
    /**
       A TCP transport.  See the documentation for ActiveMQ::Transport.

       @version $Id$
    */
    class TCPTransport : public Transport {
    public:
        /// Constructs a new TCP transport
        TCPTransport(const std::string& host, const std::string& port);
        /// Constructs a new TCP transport
        TCPTransport(const std::string& uri);
        virtual ~TCPTransport();
        /// Connects to the host/port given in the constructor
        void connect();
        /// Sends data
        int send(const unsigned char *buf, size_t count);
        /// Receives data
        int recv(unsigned char *buf, size_t count);
        /// Disconnects
        void disconnect();
        /// Checks connectivity on transport
        bool isConnected() const;
        /// Gets the underlying socket
        int getFD() const { return fd_; }

        /// dummy function to get static linkers to reference TCPTransport.o
        static int dummy();
    private:
        int fd_;
        std::string host_;
        std::string port_;
        bool connected_;

        TCPTransport(const TCPTransport &);
        TCPTransport& operator=(const TCPTransport &);
    };
};

#endif // ACTIVEMQ_TCP_TRANSPORT_H
