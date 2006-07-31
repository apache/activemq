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

#include <utility>

#include "TransportFactory.h"

#include "amq_corelib/Exception.h"
#include "amq_corelib/RCSID.h"

using namespace ActiveMQ;

using std::string;
using std::pair;
using std::map;
using std::auto_ptr;

RCSID(TransportFactory, "$Id$");

TransportFactory&
TransportFactory::instance() {
    static TransportFactory instance_;
    return instance_;
}

void
TransportFactory::registerTransport(const string& protocol,
                                    const TransportInit& initFromURI) {
    inits_.insert(pair<string, TransportInit>(protocol, initFromURI));
}

auto_ptr<Transport>
TransportFactory::getFromURI(const string& uri) {
    // get the protocol out
    string protocol(uri, 0, uri.find(":"));
    map<string, TransportInit>::iterator i = inits_.find(protocol);
    if (i == inits_.end())
        throw Exception("No transport registered for " + protocol);
    return (i->second)(uri);
}

