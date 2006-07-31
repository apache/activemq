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

#ifndef ACTIVEMQ_TRANSPORT_H
#define ACTIVEMQ_TRANSPORT_H

#include <memory>

#include "amq_corelib/Buffer.h"

namespace ActiveMQ {
    /// Interface defining a transport layer
    /**
       This class represents the fundamental operations on a logical
       transport layer.  Implementations are TCP, UDP, etc.

       @version $Id$
    */
    class Transport {
    public:
        /// Connects the transport
        /**
           Connects the fd.  Will throw ActiveMQ::Exception on error.
        */
        virtual void connect() = 0;
        
        /// Checks connectivity on transport
        /**
           @returns true if the transport is connected
        */
        virtual bool isConnected() const = 0;

        /// Sends data on the transport
        /**
          Sends data.  Will throw ActiveMQ::Exception on error.

          @param buf the data to send
          @param count the number of bytes to read from buf

          @returns the number of bytes sent
        */
        virtual int send(const unsigned char *buf, size_t count) = 0;

        /// Sends data on the transport
        /**
           Sends data.  Will throw ActiveMQ::Exception on error.

           @param buf the data to send

           @returns the number of bytes sent
        */
        int send(const Buffer& buf) { return send(&(buf.front()), buf.size()); }

        /// Reads ready data from the transport
        /**
           Receives data.  Will only block if no data is ready.
           Will throw Activemq::Exception on error.

           @param buf the buffer to write into
           @param count the maximum amount to read

           @returns the number of bytes read
        */
        virtual int recv(unsigned char *buf, size_t count) = 0;

        /// Closes the transport
        /**
           Explicitly closes the transport.
           Will throw ActiveMQ::Exception on error.
        */
        virtual void disconnect() = 0;

        /// Gets the file descriptor
        /**
           @returns the underlying file descriptor
        */
        virtual int getFD() const = 0;

        virtual ~Transport() = 0;
    };
};

#endif // ACTIVEMQ_TRANSPORT_H
