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
#include <memory>
#include <iostream>

#include "amq_brokersession/BrokerSession.h"
#include "amq_corelib/BlockingMessageConsumerRef.h"
#include "amq_corelib/TextMessage.h"

using namespace ActiveMQ;

using std::cerr;
using std::cout;
using std::endl;
using std::auto_ptr;

int msgcount = 0;

void handleException(const Exception& e) {
    cerr << "handleException: " << e.what() << endl;
}

void callback(auto_ptr<Message> msg) {
    msgcount++;
    cout << "Received message: " << msgcount << endl;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        cerr << "Usage: brokersession_example_receiver <broker_uri>" << endl;
        return 1;
    }

    try {

    BrokerSession bs(argv[1]);
//    bs.setExceptionCallback(handleException);
    bs.connect();

    BlockingMessageConsumerRef smc = bs.newBlockingMessageConsumer();
    BlockingMessageConsumerRef a, b, c, d, e;

    a = b;
    a = a;

    a = smc;
    c = a;
    c = c;
    a = a;
    BlockingMessageConsumerRef f = a;
    BlockingMessageConsumerRef g = f;

    Destination dest = bs.createTopic("test");

    bs.subscribe(dest, smc);
    
//     sleep(20);

    while (1)
        callback(smc.receive());

    } catch (const std::exception& e) {
        cout << "exception: " << e.what() << endl;
    }
    return 0;
}
