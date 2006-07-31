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

#ifndef ACTIVEMQ_TRANSPORTFACTORY_H
#define ACTIVEMQ_TRANSPORTFACTORY_H

#include <string>
#include <memory>
#include <map>

#include "Transport.h"

namespace ActiveMQ {
    class Transport;

    /// Constructs a new transport from a URI.
    /**
       This class keeps track of the mapping between transport names
       and representative classes.

       See ActiveMQ::TransportInitializer for a convenience class that can
       register your transport at static initialization time.

       @version $Id$
    */
    class TransportFactory {
    public:
        typedef std::auto_ptr<Transport> (*TransportInit)(const std::string &);

        /// Gets a singleton instance
        /**
           This function returns the global TransportFactory object that the below operations can be called on.
           
           @returns the TransportFactory instance
        */
        static TransportFactory& instance();

        /// Registers a transport
        /**
           This function defines a mapping between a protocol name
           ("tcp", "http") in a URI and an initializer that can take
           an appropriate URI and construct a Transport instance.
           This overwrites any transport class already registered for
           a particular protocol name.

           These URIs are ActiveMQ URIs.  See the ActiveMQ
           documentation for details:

           http://www.activemq.org/Configuring+Transports

           @param protocol the "protocol" portion of the URIs this transport represents
           @param initFromURI a function that can take an appropriate URI and build a transport instance (to be owned by the caller)
        */
        void registerTransport(const std::string& protocol,
                                      const TransportInit& initFromURI);

        /// Constructs a transport from a URI
        /**
           Constructs a transport by consulting the internal map of
           protocols to transports (which have registered with
           registerTransport above) and building the transport from
           the given uri.

           This returns an auto_ptr, but if that doesn't work well in
           your application, there is a dumb pointer overload.

           Throws ActiveMQ::Exception when the transport is not
           registered.
        
           @param uri the uri representing the connection
           @returns a new instance of the transport, owned by the caller
        */
        std::auto_ptr<Transport> getFromURI(const std::string& uri);

        /// Constructs a transport from a URI
        /**
           Like above, but returns a dumb pointer.  <b>It is still the
           caller's responsibility to release this memory by calling
           delete</b>.

           Throws ActiveMQ::Exception when the transport is not
           registered.
        
           @param uri the uri representing the connection
           @returns a new instance of the transport, owned by the caller
        */
        Transport *getPtrFromURI(const std::string& uri) { return getFromURI(uri).release(); }
    private:
        std::map<std::string, TransportInit> inits_;
        TransportFactory() {}
    };
};

#endif // ACTIVEMQ_TRANSPORTFACTORY_H
