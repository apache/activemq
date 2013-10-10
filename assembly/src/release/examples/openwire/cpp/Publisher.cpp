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

#include <activemq/util/Config.h>

#include <decaf/lang/System.h>
#include <decaf/lang/Runnable.h>
#include <decaf/lang/Integer.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/Destination.h>
#include <cms/MessageProducer.h>
#include <cms/TextMessage.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>

using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace decaf;
using namespace decaf::lang;

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
int main(int argc, char* argv[]) {

    activemq::library::ActiveMQCPP::initializeLibrary();

    std::cout << "=====================================================\n";
    std::cout << "Starting the Publisher example:" << std::endl;
    std::cout << "-----------------------------------------------------\n";

    std::string user = getEnv("ACTIVEMQ_USER", "admin");
    std::string password = getEnv("ACTIVEMQ_PASSWORD", "password");
    std::string host = getEnv("ACTIVEMQ_HOST", "localhost");
    int port = Integer::parseInt(getEnv("ACTIVEMQ_PORT", "61616"));
    std::string destination = getArg(argv, argc, 1, "event");

    int messages = 10000;
    int size = 256;

    std::string DATA = "abcdefghijklmnopqrstuvwxyz";
    std::string body = "";
    for( int i=0; i < size; i ++) {
        body += DATA.at(i%DATA.length());
    }

    {
        ActiveMQConnectionFactory factory;
        factory.setBrokerURI(std::string("tcp://") + host + ":" + Integer::toString(port));

        std::auto_ptr<TextMessage> message;
        std::auto_ptr<Connection> connection(factory.createConnection(user, password));

        connection->start();

        std::auto_ptr<Session> session(connection->createSession());
        std::auto_ptr<Destination> dest(session->createTopic(destination));
        std::auto_ptr<MessageProducer> producer(session->createProducer(dest.get()));

        producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

        for( int i=1; i <= messages; i ++) {
            message.reset(session->createTextMessage(body));
            producer->send(message.get());
            if( (i % 1000) == 0) {
                std::cout << "Sent " << i << " messages" << std::endl;;
            }
        }

        message.reset(session->createTextMessage("SHUTDOWN"));
        producer->send(message.get());

        connection->close();
    }

    std::cout << "-----------------------------------------------------\n";
    std::cout << "Finished with the example." << std::endl;
    std::cout << "=====================================================\n";

    activemq::library::ActiveMQCPP::shutdownLibrary();
}
