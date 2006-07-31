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

#ifndef ACTIVEMQ_CORELIB_IMPL_H
#define ACTIVEMQ_CORELIB_IMPL_H

#include <string>
#include <map>
#include <list>
#include <memory>

#include <boost/shared_ptr.hpp>

#include "Destination.h"

#include "Message.h"

#include "Buffer.h"
#include "BlockingMessageConsumerRef.h"
#include "NonBlockingMessageConsumerRef.h"
#include "Logger.h"
#include "UUIDGenerator.h"

#include "marshal/ProtocolFormat.h"

namespace ActiveMQ {
    namespace Command {
        class BaseCommand;
        class AbstractCommand;
        class ProducerId;
        class ConsumerId;
    };

    class CoreLib;
    /// Private implementation class for CoreLib
    class CoreLibImpl {
    public:
        virtual ~CoreLibImpl();
    private:
        friend class CoreLib;
        CoreLibImpl(CoreLib *parent,
                    const std::string& user,
                    const std::string& password);
        void initialize(Buffer& b);
        void disconnect(Buffer& b);
        void publish(const Message& msg, Buffer& b);
        void publish(const Destination& dest,
                     const Message& msg,
                     Buffer& b);
        void subscribe(const Destination& dest,
                       MessageConsumerRef& q,
                       Buffer& b);
        void unsubscribe(const Destination& dest,
                         Buffer& b);
        void handleData(const Buffer& incoming, Buffer& b);
        void handleData(const uint8_t *buf, size_t len, Buffer& b);
        NonBlockingMessageConsumerRef newNonBlockingMessageConsumer();
        BlockingMessageConsumerRef newBlockingMessageConsumer();
        void setLogger(std::auto_ptr<Logger> lgr);
        Logger& getLogger();
        Destination createTemporaryTopic();
        Destination createTemporaryQueue();
        Destination createTopic(const std::string& name);
        Destination createQueue(const std::string& name);
        void registerDest(const Destination& d);
        void unregisterDest(const Destination& d);

        CoreLibImpl(const CoreLibImpl &);
        CoreLibImpl& operator=(const CoreLibImpl &);

        CoreLib *parent_; // for passing to new MessageConsumerRef objects and destinations

        const std::string user_;
        const std::string password_;
        std::map<Destination, MessageConsumer *> destinationMaps_;
        std::list<MessageConsumer *> consumers_;
        std::list<MessageConsumerRef *> consumerRefs_;
        std::map<MessageConsumer *, int> refCounts_;

        // Destinations are reference-counted
        std::map<std::string, int> destRefCounts_;
        std::list<const Destination*> allRegisteredDestinations_;

        // State of asynchronous message unmarshalling
        std::vector<uint8_t> unmarshalBuffer_;
        uint8_t sizeBuf_[4];
        uint8_t sizeBufPos_;
        
        // Operations that don't have a way to pass up outgoing data
        // can use the pending buffer.
        std::vector<uint8_t> pendingBuffer_;
        void marshalPending_(Buffer& b);
        
        // Unmarshals a full buffer containing one message
        void unmarshalBuffer(std::vector<uint8_t>& buf, Buffer& b);
        // internal data used to track message unmarshalling state
        bool inMessage_;
        size_t yetToRecv_;

        Marshalling::ProtocolFormat pf_;

        // We only keep one connection / session open for the entire time
        int64_t sessionId_;
        std::string clientId_;
        std::string connectionId_;
        // Only one JMS producer is used for all outgoing messages
        boost::shared_ptr<Command::ProducerId> producerId_;

        // subject -> consumer id mapping
        std::map<Destination, boost::shared_ptr<Command::ConsumerId> > consumerIds_;

        // keep track of ids to use in allocation of new objects
        int nextCommandId_;
        int nextProducerId_;
        int nextConsumerId_;
        int nextTempDestId_;

        // Marshal a command into a buffer
        void marshalCommand_(Command::AbstractCommand& o, Buffer& b);
        void marshalCommand_(Command::BaseCommand& o, Buffer& b);

        // Unmarshal a command from a buffer
        void unmarshalCommand_(Command::AbstractCommand& o, const Buffer& b);

        std::auto_ptr<Logger> logger_;

        void registerRef(MessageConsumerRef *mc);
        void deregisterRef(MessageConsumerRef *mc);

        bool initialized_;
    };
};

#endif // ACTIVEMQ_CORELIB_IMPL_H
