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

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>

#include <iostream>
#include <memory>

#include "amq_brokersession/BrokerSession.h"
#include "amq_corelib/NonBlockingMessageConsumerRef.h"
#include "amq_corelib/TextMessage.h"

using namespace std;
using namespace ActiveMQ;

void callback(auto_ptr<Message> msg) {
    cout << "Received message: " << static_cast<TextMessage *>(msg.get())->getText() << endl;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        cerr << "Usage: brokersession_example_receiver_async <broker_uri>" << endl;
        return 1;
    }
    try {
        BrokerSession bs(argv[1]);
        bs.connect();

        NonBlockingMessageConsumerRef smc;

        smc = bs.newNonBlockingMessageConsumer();

        smc.getEventFD();

        Destination d = bs.createTopic("test");
        bs.subscribe(d, smc);

        sleep(2);

        fd_set fds;
        FD_ZERO(&fds);
        while (1) {
            FD_SET(smc.getEventFD(),&fds);
            int rc = select(smc.getEventFD()+1,&fds,NULL,NULL,NULL);
            if (rc == 1) {
                auto_ptr<Message> m = smc.receive();
                callback(m);
            }
            else if (rc == -1 && errno != EINTR) {
                perror("select");
                return 1;
            }
        }
    } catch (const Exception &e) {
        cerr << "Exception: " << e.what() << endl;
        return 1;
    } catch (...) {
        cerr << "Some random exception!" << endl;
    }
    
    return 0;
}
