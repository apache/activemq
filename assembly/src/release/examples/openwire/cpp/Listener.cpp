/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Integer.h>
#include <decaf/lang/System.h>
#include <activemq/util/Config.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/Destination.h>
#include <cms/MessageProducer.h>
#include <cms/TextMessage.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>

using namespace activemq;
using namespace activemq::core;
using namespace decaf::lang;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
std::string getEnv(const std::string& key, const std::string& defaultValue) {

    try{
        return System::getenv(key);
    } catch(...) {
    }

    return defaultValue;
}

////////////////////////////////////////////////////////////////////////////////
std::string getArg(char* argv[], int argc, int index, const std::string& defaultValue) {

    if( index < argc ) {
        return argv[index];
    }

    return defaultValue;
}

////////////////////////////////////////////////////////////////////////////////
int main(int argc AMQCPP_UNUSED, char* argv[] AMQCPP_UNUSED) {

    activemq::library::ActiveMQCPP::initializeLibrary();

    std::cout << "=====================================================\n";
    std::cout << "Starting the Listener example:" << std::endl;
    std::cout << "-----------------------------------------------------\n";

    std::string user = getEnv("ACTIVEMQ_USER", "admin");
    std::string password = getEnv("ACTIVEMQ_PASSWORD", "password");
    std::string host = getEnv("ACTIVEMQ_HOST", "localhost");
    int port = Integer::parseInt(getEnv("ACTIVEMQ_PORT", "61616"));
    std::string destination = getArg(argv, argc, 1, "event");

    {
        ActiveMQConnectionFactory factory;
        factory.setBrokerURI(std::string("tcp://") + host + ":" + Integer::toString(port));

        std::auto_ptr<Connection> connection(factory.createConnection(user, password));

        std::auto_ptr<Session> session(connection->createSession());
        std::auto_ptr<Destination> dest(session->createTopic(destination));
        std::auto_ptr<MessageConsumer> consumer(session->createConsumer(dest.get()));

        connection->start();

        long long start = System::currentTimeMillis();
        long long count = 0;

        std::cout << "Waiting for messages..." << std::endl;
        while(true) {

            std::auto_ptr<Message> message(consumer->receive());

            const TextMessage* txtMsg = dynamic_cast<const TextMessage*>(message.get());

            if( txtMsg != NULL ) {
                std::string body = txtMsg->getText();
                if( body == "SHUTDOWN" ) {
                    long long diff = System::currentTimeMillis() - start;
                    cout << "Received " << count << " in " << (double)diff/1000.0 << " seconds" << std::endl;
                    break;
                } else {
                    if( count == 0 ) {
                        start = System::currentTimeMillis();
                    }
                    count++;
                    if( count % 1000 == 0 ) {
                        std::cout << "Received " << count << " messages." << std::endl;
                    }
                }

            } else {
                std::cout << "Unexpected message type." << std::endl;
            }
        }

        connection->close();
    }

    std::cout << "-----------------------------------------------------\n";
    std::cout << "Finished with the example." << std::endl;
    std::cout << "=====================================================\n";

    activemq::library::ActiveMQCPP::shutdownLibrary();
}
