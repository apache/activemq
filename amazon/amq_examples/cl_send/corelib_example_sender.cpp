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

#include <unistd.h>

#include <iostream>
#include <memory>

#include "amq_corelib/CoreLib.h"
#include "amq_corelib/TextMessage.h"

#include "amq_transport/Transport.h"
#include "amq_transport/TransportFactory.h"

using namespace std;
using namespace ActiveMQ;

int main(int argc, char **argv) {
    if (argc < 3) {
        cerr << "Usage: corelib_example_sender <broker_uri> <num_to_send>" << endl;
        return 1;
    }
    CoreLib cl("","");
    Transport *t = TransportFactory::instance().getPtrFromURI(argv[1]);
    t->connect();
    Buffer conn;
    cl.initialize(conn);
    t->send(conn);
    Destination d = cl.createTopic("test");
    for (int i = 0; i < atoi(argv[2]); i++) {
        Buffer msg;
        cl.publish(d,TextMessage("test message"),msg);
        t->send(msg);
        sleep(2);
    }
    delete t;
    return 0;
}
