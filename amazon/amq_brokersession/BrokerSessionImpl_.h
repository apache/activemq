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

#ifndef ACTIVEMQ_BROKERSESSION_IMPL_H
#define ACTIVEMQ_BROKERSESSION_IMPL_H

#include <memory>

#include "amq_corelib/CoreLib.h"
#include "amq_corelib/ExceptionCallback.h"
#include "amq_corelib/Logger.h"

namespace ActiveMQ {
    class BrokerSession;
    class NonBlockingMessageConsumerRef;
    class BlockingMessageConsumerRef;
    class MessageConsumer;
    class Transport;
    class Destination;
    class Message;

    /// Private implementation class for BrokerSession.
    class BrokerSessionImpl {
    public:
        virtual ~BrokerSessionImpl();
    private:
        friend class BrokerSession;
        BrokerSessionImpl(const std::string& uri,
                          const std::string& user = "",
                          const std::string& password = "");
        void connect();
        bool isConnected() const;
        void disconnect();
        void publish(const Message& msg);
        void publish(const Destination& dest, const Message& msg);
        void subscribe(const Destination& dest, MessageConsumerRef& q);
        void unsubscribe(const Destination& dest);
        NonBlockingMessageConsumerRef newNonBlockingMessageConsumer();
        BlockingMessageConsumerRef newBlockingMessageConsumer();
        ExceptionCallback setExceptionCallback(ExceptionCallback c);
        ExceptionCallback& getExceptionCallback();
        void setLogger(std::auto_ptr<Logger> lgr);
        Destination createTemporaryTopic();
        Destination createTemporaryQueue();
        Destination createTopic(const std::string& name);
        Destination createQueue(const std::string& name);

        CoreLib corelib_;
        std::auto_ptr<Transport> transport_;
        bool connected_;

        pthread_t backgroundThread_;
        mutable pthread_mutex_t brokersessionMutex_;

        ExceptionCallback exceptionCB_;

        static void *backgroundThreadTask_(void *arg);

        void fatal_(const std::string& msg);
        void error_(const std::string& msg);
        void warning_(const std::string& msg);
        void inform_(const std::string& msg);
        void debug_(const std::string& msg);

        BrokerSessionImpl(const BrokerSessionImpl &);
        BrokerSession& operator=(const BrokerSession &);
    };
};

#endif // ACTIVEMQ_BROKERSESSION_IMPL_H
