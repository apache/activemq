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

#ifndef ACTIVEMQ_TRANSPORT_INITIALIZER_H
#define ACTIVEMQ_TRANSPORT_INITIALIZER_H

#include <string>

#include "TransportFactory.h"

namespace ActiveMQ {
    /// Convenience class for static registration of Transport subclasses.
    /**
       This class is useful as a static member of your
       ActiveMQ::Transport subclass.  The constructor will be called
       at static initialization time.  See the ActiveMQ::TCPTransport
       code for an example of this.

       @version $Id$
    */
    class TransportInitializer {
    public:
        TransportInitializer(const std::string& protocol,
                             const TransportFactory::TransportInit& func);
    };
};

#endif // ACTIVEMQ_TRANSPORT_INITIALIZER_H
