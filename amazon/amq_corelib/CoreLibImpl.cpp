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

#include <netinet/in.h>
#include <unistd.h>

#include "RCSID.h"
#include <algorithm>
#include <string>
#include <sstream>
#include <list>
#include <utility>
#include <iostream>

#include "TextMessage.h"
#include "BytesMessage.h"

#include "PrimitiveMap.h"
#include "MessageConsumer.h"
#include "NonBlockingMessageConsumer.h"
#include "BlockingMessageConsumer.h"
#include "NonBlockingMessageConsumerRef.h"
#include "BlockingMessageConsumerRef.h"
#include "Exception.h"
#include "Destination.h"

#include "CoreLibImpl_.h"

#include "StompMessage.h"
#include "NullLogger.h"

#include "command/CommandTypes.h"
#include "command/ConnectionInfo.h"
#include "command/SessionInfo.h"
#include "command/ConsumerInfo.h"
#include "command/WireFormatInfo.h"
#include "command/ProducerInfo.h"
#include "command/ShutdownInfo.h"
#include "command/ActiveMQTopic.h"
#include "command/ActiveMQTempTopic.h"
#include "command/ActiveMQTempQueue.h"
#include "command/ActiveMQQueue.h"
#include "command/ActiveMQTextMessage.h"
#include "command/ActiveMQBytesMessage.h"
#include "command/ActiveMQDestination.h"
#include "command/MessageDispatch.h"
#include "command/Response.h"
#include "command/BrokerInfo.h"
#include "command/RemoveInfo.h"
#include "command/DestinationInfo.h"
#include "command/MessageAck.h"
#include "marshal/BaseDataStreamMarshaller.h"
#include "marshal/ProtocolFormat.h"
#include "marshal/MarshallerFactory.h"
#include "marshal/BufferWriter.h"
#include "marshal/BufferReader.h"

using namespace ActiveMQ;
using namespace std;
using boost::shared_ptr;

RCSID(CoreLibImpl, "$Id$");

#define FATAL(msg) \
    do { if (logger_.get() && logger_->isEnabled(LogLevel::Fatal)) \
        logger_->logFatal(msg); } while(0)

#define ERROR(msg) \
    do { if (logger_.get() && logger_->isEnabled(LogLevel::Error)) \
        logger_->logError(msg); } while(0)

#define WARNING(msg) \
    do { if (logger_.get() && logger_->isEnabled(LogLevel::Warning)) \
        logger_->logWarning(msg); } while(0)

#define INFORM(msg) \
    do { if (logger_.get() && logger_->isEnabled(LogLevel::Inform)) \
        logger_->logInform(msg); } while(0)

#define DEBUG(msg) \
    do { if (logger_.get() && logger_->isEnabled(LogLevel::Debug)) \
        logger_->logDebug(msg); } while(0)

const static int OPENWIRE_VERSION = 1;
const static int PREFETCH_SIZE    = 32766;
const static int SESSION_ID       = 12345;

const static int STANDARD_ACK     = 2;

const static int ADD_DEST         = 0;
const static int REMOVE_DEST      = 1;

const static UUIDGenerator uuidgen;

const static string activemq = "ActiveMQ";

CoreLibImpl::CoreLibImpl(CoreLib *parent,
                         const string& user,
                         const string& password) :
    parent_(parent),
    user_(user),
    password_(password),
    sizeBufPos_(0),
    inMessage_(false),
    yetToRecv_(0),
    sessionId_(SESSION_ID),
    nextCommandId_(0),
    nextProducerId_(0),
    nextConsumerId_(0),
    nextTempDestId_(0),
    logger_(new NullLogger()),
    initialized_(false) {

    Marshalling::MarshallerFactory::configure(pf_);
    clientId_ = uuidgen.getGuid();
    connectionId_ = uuidgen.getGuid();
}

void
CoreLibImpl::marshalPending_(Buffer& b) {
    if (!(pendingBuffer_.empty())) {
        b.insert(b.end(), pendingBuffer_.begin(), pendingBuffer_.end());
        pendingBuffer_.resize(0);
    }
}

void
CoreLibImpl::marshalCommand_(Command::BaseCommand& o, Buffer& b) {
    o.setCommandId(nextCommandId_++);
    marshalCommand_(static_cast<Command::AbstractCommand&>(o), b);
}

void loosemarshalcommand(Command::WireFormatInfo& wfi, Buffer& b) {
    size_t length = htonl(100);
    int openwire_version = htonl(OPENWIRE_VERSION);
    // size
    //b.insert(b.end(), (uint8_t *)(&length), (uint8_t *)(&length) + 4);
    // type
    b.push_back(1);
    // magic
    b.push_back('A');
    b.push_back('c');
    b.push_back('t');
    b.push_back('i');
    b.push_back('v');
    b.push_back('e');
    b.push_back('M');
    b.push_back('Q');
    // openwire version
    b.insert(b.end(), (uint8_t *)(&openwire_version), (uint8_t *)(&openwire_version) + 4);
    // marshalled properties
    // true it's there
    b.push_back(1);
    size_t mplen = htonl(wfi.getMarshalledProperties().size());
    // length
    b.insert(b.end(), (uint8_t *)(&mplen), (uint8_t *)(&mplen) + 4);
    // data
    b.insert(b.end(), wfi.getMarshalledProperties().begin(), wfi.getMarshalledProperties().end());

    length = htonl(1 + 8 + 4 + 1 + 4 + ntohl(mplen));
    b.insert(b.begin(), (uint8_t *)(&length), (uint8_t *)(&length) + 4);

}

void
CoreLibImpl::marshalCommand_(Command::AbstractCommand& o, Buffer& b) {
    IO::BooleanStream bs;

    // get the marshaller
    Marshalling::BaseDataStreamMarshaller* marshaller = pf_.getMarshaller(o.getCommandType());
    if (marshaller == NULL) {
        stringstream exmsg("Invalid command type (");
        exmsg << o.getCommandType();
        exmsg << ") passed to marshalCommand";
        throw Exception(exmsg.str());
    }
    
    // first pass gets the size and writes the flags bitset
    uint32_t size = marshaller->marshal1(pf_, o, bs);
    size = htonl(size + bs.getMarshalledSize());

    // first 4 bytes of a message are its size
    b.insert(b.end(), (uint8_t *)(&size), (uint8_t *)(&size) + 4);

    // then one byte that is the command type
    b.push_back(o.getCommandType());
    
    IO::BufferWriter bw(b);
    // then the bitset
    bs.marshal(bw);

    // then the second marshalling pass which actually writes the data out
    marshaller->marshal2(pf_, o, bw, bs);
}

void
CoreLibImpl::unmarshalCommand_(Command::AbstractCommand& o, const Buffer& b) {
    IO::BufferReader br(b);
    IO::BooleanStream bs(br);

    Marshalling::BaseDataStreamMarshaller *marshaller = pf_.getMarshaller(o.getCommandType());
    if (marshaller == NULL) {
        stringstream exmsg("Invalid command type (");
        exmsg << o.getCommandType();
        exmsg << ") received from broker";
        throw Exception(exmsg.str());
    }
    marshaller->unmarshal(pf_, o, br, bs);
}

void
CoreLibImpl::initialize(Buffer& b) {
    // The first step is wire format negotiation - though it's not really -
    // see https://issues.apache.org/activemq/browse/AMQ-681

    // In any case, the first packet sent is a WireFormatInfo packet

    Command::WireFormatInfo wfi;

    // It has a magic string prefix
    vector<uint8_t> magic(activemq.begin(), activemq.end());
    wfi.setMagic(magic);

    // OpenWire version
    wfi.setVersion(OPENWIRE_VERSION);

    // We want "tight" encoding, which is length-prefixed
    PrimitiveMap optionMap;
    optionMap.putBoolean("TightEncodingEnabled", true);

    Buffer marshalledOptions;
    optionMap.marshal(marshalledOptions);
    
    wfi.setMarshalledProperties(marshalledOptions);
    
//    marshalCommand_(wfi, b);
    loosemarshalcommand(wfi, b);

    // The next command sets up our JMS Connection

    Command::ConnectionInfo cinfo;
    cinfo.setUserName(user_);
    cinfo.setPassword(password_);
    cinfo.setClientId(clientId_);

    shared_ptr<Command::ConnectionId> cid(new Command::ConnectionId());
    cid->setValue(connectionId_);
    cinfo.setConnectionId(cid);

    marshalCommand_(cinfo, b);

    // Now we set up the JMS Session

    Command::SessionInfo sinfo;

    shared_ptr<Command::SessionId> sid(new Command::SessionId());
    sid->setConnectionId(connectionId_);
    sid->setValue(sessionId_);

    sinfo.setSessionId(sid);
    
    marshalCommand_(sinfo, b);

    initialized_ = true;
}

void
CoreLibImpl::publish(const Message& m, Buffer& b) {
    publish(m.getDestination(), m, b);
}

void
CoreLibImpl::publish(const Destination& d,
                     const Message& m,
                     Buffer &b) {

    // Offload any pending data now that we have the opportunity
    marshalPending_(b);

    shared_ptr<Command::ActiveMQDestination> dest(d.createCommandInstance().release());

    // Create our producer if we haven't already
    if (producerId_.get() == NULL) {
        Command::ProducerInfo pi;

        shared_ptr<Command::ProducerId> pid(new Command::ProducerId());
        pid->setConnectionId(connectionId_);
        pid->setSessionId(sessionId_);
        pid->setValue(nextProducerId_++);
        producerId_ = pid;

        pi.setProducerId(producerId_);
        marshalCommand_(pi, b);
    }

    auto_ptr<Command::Message> message;

    if (m.getType() == Command::Types::ACTIVEMQ_TEXT_MESSAGE)
        message.reset(new Command::ActiveMQTextMessage());
    else if (m.getType() == Command::Types::ACTIVEMQ_BYTES_MESSAGE)
        message.reset(new Command::ActiveMQBytesMessage());

    message->setProducerId(producerId_);
    message->setDestination(dest);
    message->setPersistent(false);

    if (!(m.getReplyTo().getName().empty())) {
        shared_ptr<Command::ActiveMQDestination> replyTo(m.getReplyTo().createCommandInstance().release());
        message->setReplyTo(replyTo);
    }

    shared_ptr<Command::MessageId> mid(new Command::MessageId());

    mid->setProducerId(producerId_);
    message->setMessageId(mid);

    Buffer content;
    // serialize the message into the buffer
    m.marshall(content);

    message->setContent(content);

    marshalCommand_(*message, b);
}

void
CoreLibImpl::disconnect(Buffer& b) {
    marshalPending_(b);

    // remove the sessionid

    Command::RemoveInfo r;
    shared_ptr<Command::SessionId> sidptr(new Command::SessionId());
    sidptr->setValue(sessionId_);
    sidptr->setConnectionId(connectionId_);
    r.setObjectId(sidptr);
    marshalCommand_(r, b);

    // remove the connectionid

    Command::RemoveInfo r2;
    shared_ptr<Command::ConnectionId> cidptr(new Command::ConnectionId());
    cidptr->setValue(connectionId_);
    r2.setObjectId(cidptr);
    marshalCommand_(r2, b);
    
    // send shutdowninfo

    Command::ShutdownInfo si;
    marshalCommand_(si, b);
    initialized_ = false;
}

CoreLibImpl::~CoreLibImpl() {
    for (list<MessageConsumerRef *>::iterator i = consumerRefs_.begin();
         i != consumerRefs_.end(); ++i) {
        (*i)->invalidate();
        deregisterRef(*i);
    }
    for (list<const Destination *>::iterator i = allRegisteredDestinations_.begin();
         i != allRegisteredDestinations_.end(); ++i) {
        const_cast<Destination*>(*i)->invalidate();
    }
}

void
CoreLibImpl::subscribe(const Destination& d, MessageConsumerRef& q, Buffer& b) {
    marshalPending_(b);

    MessageConsumer *mcptr = q.getConsumer();

    destinationMaps_.insert(pair<Destination, MessageConsumer *>(d, mcptr));

    if (consumerIds_.count(d) == 0) {
        // make a new consumer id if there isn't one for this destination
        shared_ptr<Command::ConsumerId> cid(new Command::ConsumerId());
        cid->setConnectionId(connectionId_);
        cid->setSessionId(sessionId_);
        cid->setValue(nextConsumerId_++);
        consumerIds_[d] = cid;
    }

    Command::ConsumerInfo si;
    shared_ptr<Command::ActiveMQDestination> dest(d.createCommandInstance().release());

    si.setConsumerId(consumerIds_[d]);
    si.setDestination(dest);
    si.setPrefetchSize(PREFETCH_SIZE);
    si.setDispatchAsync(true);

    marshalCommand_(si, b);
}

void
CoreLibImpl::unsubscribe(const Destination& d, Buffer& b) {
    marshalPending_(b);

    map<Destination, MessageConsumer*>::iterator i = destinationMaps_.find(d);
    if (i == destinationMaps_.end())
        throw Exception("Not subscribed to destination " + d.getName());
    i->second->removeQueued(d);

    if (consumerIds_.count(d) != 0) {

        // Remove the consumer for this destination
        
        map<Destination, shared_ptr<Command::ConsumerId> >::iterator di = consumerIds_.find(d);
        shared_ptr<Command::IDataStructure> ourcid(di->second);
        Command::RemoveInfo ri;
        ri.setObjectId(ourcid);
        
        marshalCommand_(ri, b);
        
        consumerIds_.erase(di);
    }
}

void
CoreLibImpl::handleData(const Buffer& incoming, Buffer& b) {
    handleData(&(incoming.operator[](0)), incoming.size(), b);
}

void
CoreLibImpl::unmarshalBuffer(vector<uint8_t>& buf, Buffer& b) {
    int type = buf[0];

    buf.erase(buf.begin());

    switch (type) {
    case Command::Types::BROKER_INFO: {
        // BrokerInfo
        // Ignore it, nothing to really do with it
    }
        break;
    case Command::Types::SHUTDOWN_INFO: {
        // ShutdownInfo, broker is exiting
        Command::ShutdownInfo s;

        unmarshalCommand_(s, buf);

        INFORM("Got ShutdownInfo, shutting down");
        disconnect(b);
    }
        break;
    case Command::Types::MESSAGE_DISPATCH: {
        // MessageDispatch, new message arriving
        Command::MessageDispatch md;
        
        unmarshalCommand_(md, buf);

        shared_ptr<const Command::Message> m = md.getMessage();
        if (m.get() != NULL) {
            shared_ptr<const Command::ActiveMQDestination> dest = m->getDestination();
            if (dest.get() != NULL) {

                // Ack the message

                Command::MessageAck ack;

                auto_ptr<Command::ActiveMQDestination> destToAck;
                Destination myd;

                switch(dest->getCommandType()) {
                case Command::Types::ACTIVEMQ_TOPIC:
                    destToAck.reset(new Command::ActiveMQTopic());
                    myd = Destination(parent_, dest->getPhysicalName(), false /* isTemp */, true /* isTopic */);
                    break;
                case Command::Types::ACTIVEMQ_QUEUE:
                    destToAck.reset(new Command::ActiveMQQueue());
                    myd = Destination(parent_, dest->getPhysicalName(), false /* isTemp */, false /* isTopic */);
                    break;
                case Command::Types::ACTIVEMQ_TEMP_TOPIC:
                    destToAck.reset(new Command::ActiveMQTempTopic());
                    myd = Destination(parent_, dest->getPhysicalName(), true /* isTemp */, true /* isTopic */);
                    break;
                case Command::Types::ACTIVEMQ_TEMP_QUEUE:
                    destToAck.reset(new Command::ActiveMQTempQueue());
                    myd = Destination(parent_, dest->getPhysicalName(), true /* isTemp */, false /* isTopic */);
                    break;
                };

                destToAck->setPhysicalName(dest->getPhysicalName());

                shared_ptr<Command::ConsumerId> cidToAck(consumerIds_[myd]);

                ack.setConsumerId(cidToAck);
                ack.setDestination(shared_ptr<Command::ActiveMQDestination>(destToAck));
                ack.setAckType(STANDARD_ACK); // tells broker to discard the message
                ack.setMessageCount(1);
                marshalCommand_(ack, b);

                const string& ourdestname(dest->getPhysicalName());

                map<Destination, MessageConsumer *>::iterator i = destinationMaps_.find(myd);
                if (i == destinationMaps_.end())
                    WARNING("No MessageConsumer registered for received message on destination " + ourdestname);
                else {
                    int mtype = m->getCommandType();
                    auto_ptr<Message> toenqueue;
                    switch (mtype) {
                    case Command::Types::ACTIVEMQ_TEXT_MESSAGE:
                        toenqueue.reset(new TextMessage(m->getContent()));
                        break;
                    case Command::Types::ACTIVEMQ_BYTES_MESSAGE:
                        toenqueue.reset(new BytesMessage(m->getContent()));
                        break;
                    default:
                        WARNING("Unknown/unimplemented message command type");
                        return;
                    };

                    toenqueue->setDestination(myd);

                    if (m->getReplyTo() != NULL) {
                        const bool rtoIsTemp =  m->getReplyTo()->getCommandType() == Command::Types::ACTIVEMQ_TEMP_QUEUE ||
                                             m->getReplyTo()->getCommandType() == Command::Types::ACTIVEMQ_TEMP_TOPIC;

                        const bool rtoIsTopic = m->getReplyTo()->getCommandType() == Command::Types::ACTIVEMQ_TEMP_TOPIC ||
                                             m->getReplyTo()->getCommandType() == Command::Types::ACTIVEMQ_TOPIC;

                        Destination rto(parent_, m->getReplyTo()->getPhysicalName(), rtoIsTemp, rtoIsTopic);

                        toenqueue->setReplyTo(rto);
                    }

                    i->second->enqueue(toenqueue.release());
                }
            }
        }
    }
        break;
    }
}

void
CoreLibImpl::handleData(const uint8_t *buf, size_t len, Buffer& b) {
    if (!inMessage_) {
        uint32_t msgsize = 0;
        if (sizeBufPos_ == 0) {
            if (len >= 4) {
                msgsize = ntohl(*(uint32_t*)(buf));
                buf += 4;
                len -= 4;
            }
            else {
                memcpy(sizeBuf_, buf, len);
                sizeBufPos_ += len;
                return;
            }
        }
        else {
            if (len + sizeBufPos_ >= 4) {
                memcpy(sizeBuf_ + sizeBufPos_, buf, 4 - sizeBufPos_);
                msgsize = ntohl(*(uint32_t*)(sizeBuf_));
                buf += (4 - sizeBufPos_);
                len -= (4 - sizeBufPos_);
            }
            else {
                memcpy(sizeBuf_ + sizeBufPos_, buf, len);
                sizeBufPos_ += len;
                return;
            }
        }

        if (msgsize == 0)
            throw Exception("Zero-length message - corrupt data stream");

        unmarshalBuffer_.resize(0);
        unmarshalBuffer_.reserve(msgsize);
        yetToRecv_ = msgsize;
        inMessage_ = true;
        sizeBufPos_ = 0;
    }

    // does this incoming message fill or exceed the buffer?

    if (len == yetToRecv_) { // exact match
        inMessage_ = false;
        unmarshalBuffer_.insert(unmarshalBuffer_.end(), buf, buf + len);
        unmarshalBuffer(unmarshalBuffer_, b);
    }
    else if (len < yetToRecv_) { // not yet
        unmarshalBuffer_.insert(unmarshalBuffer_.end(), buf, buf + len);
        yetToRecv_ -= len;
    }
    else if (len > yetToRecv_) { // too much
        inMessage_ = false;
        size_t excess = len - yetToRecv_; // number of excess bytes past the current message
        size_t thismsg = len - excess; // number of bytes of this buffer that are the current message
        unmarshalBuffer_.insert(unmarshalBuffer_.end(), buf, buf + thismsg);
        unmarshalBuffer(unmarshalBuffer_, b);
        handleData(buf + thismsg, excess, b);
    }
}

BlockingMessageConsumerRef
CoreLibImpl::newBlockingMessageConsumer(void) {
    BlockingMessageConsumer *q = new BlockingMessageConsumer();
    consumers_.push_back(q);
    refCounts_[q] = 0;
    return BlockingMessageConsumerRef(parent_, q);
}

NonBlockingMessageConsumerRef
CoreLibImpl::newNonBlockingMessageConsumer(void) {
    NonBlockingMessageConsumer *q = new NonBlockingMessageConsumer();
    consumers_.push_back(q);
    refCounts_[q] = 0;
    return NonBlockingMessageConsumerRef(parent_, q);
}

void
CoreLibImpl::registerRef(MessageConsumerRef *q) {
    if (NULL == q)
        return;

    MessageConsumer *mq = q->getConsumer();
    consumerRefs_.push_back(q);
    ++refCounts_[mq];
}

void
CoreLibImpl::deregisterRef(MessageConsumerRef *q) {

    if (NULL == q)
        return;
    MessageConsumer *mq = q->getConsumer();
    int count = --refCounts_[mq];
    if (count == 0) {
        refCounts_.erase(mq);
        // delete all references to the consumer
        for(map<Destination, MessageConsumer *>::iterator i = destinationMaps_.begin();
            i != destinationMaps_.end();) {
            if (i->second == mq)
                destinationMaps_.erase(i++);
            else
                ++i;
        }
        consumers_.remove(mq);
        // delete the consumer
        delete mq;
    }
    consumerRefs_.remove(q);
}

void
CoreLibImpl::setLogger(auto_ptr<Logger> lgr) {
    logger_ = lgr;
}

Logger&
CoreLibImpl::getLogger() {
    return *logger_;
}

void
CoreLibImpl::registerDest(const Destination& d) {
    map<string, int>::iterator i = destRefCounts_.find(d.toString());
    if (i == destRefCounts_.end())
        destRefCounts_.insert(pair<string,int>(d.toString(), 1));
    else
        i->second++;

    allRegisteredDestinations_.push_back(&d);
}

void
CoreLibImpl::unregisterDest(const Destination& d) {
    map<std::string, int>::iterator i = destRefCounts_.find(d.toString());
    if (i == destRefCounts_.end())
        return;

    allRegisteredDestinations_.remove(&d);

    if (i->second == 1) {
        // This is the last reference to this destination

        map<Destination, shared_ptr<Command::ConsumerId> >::iterator cii = consumerIds_.find(d);
        // delete the relevant consumer, if there is one
        if (cii != consumerIds_.end()) {
            shared_ptr<Command::IDataStructure> ci(cii->second);
            Command::RemoveInfo r;
            r.setObjectId(ci);
            marshalCommand_(r, pendingBuffer_);

            consumerIds_.erase(cii);
        }

        // delete the destination, if it's our responsibility

        // we know we created a temporary destination if our connection ID is in it
        if (d.isTemporary() && d.getName().find(connectionId_) != string::npos) {
            Command::DestinationInfo di;

            shared_ptr<Command::ConnectionId> cid(new Command::ConnectionId());
            cid->setValue(connectionId_);
            shared_ptr<Command::ActiveMQDestination> dest(d.createCommandInstance().release());
            di.setDestination(dest);
            di.setOperationType(REMOVE_DEST);
            di.setConnectionId(cid);

            marshalCommand_(di, pendingBuffer_);
        }
        destRefCounts_.erase(i);
    }
    else
        i->second--;
}

Destination
CoreLibImpl::createTemporaryTopic() {
    stringstream str;
    str << connectionId_ << ":" << nextTempDestId_++;
    Destination ret(parent_, str.str(), true /* isTemp */, true /* isTopic */);

    Command::DestinationInfo di;

    shared_ptr<Command::ConnectionId> cid(new Command::ConnectionId());
    cid->setValue(connectionId_);

    shared_ptr<Command::ActiveMQDestination> dest(new Command::ActiveMQTempTopic());
    dest->setPhysicalName(ret.getName());

    di.setDestination(dest);
    di.setOperationType(ADD_DEST);
    di.setConnectionId(cid);

    marshalCommand_(di, pendingBuffer_);

    return ret;
}

Destination
CoreLibImpl::createTemporaryQueue() {
    stringstream str;
    str << connectionId_ << ":" << nextTempDestId_++;
    Destination ret(parent_, str.str(), true /* isTemp */, false /* isTopic */);

    Command::DestinationInfo di;

    shared_ptr<Command::ConnectionId> cid(new Command::ConnectionId());
    cid->setValue(connectionId_);

    shared_ptr<Command::ActiveMQDestination> dest(new Command::ActiveMQTempQueue());
    dest->setPhysicalName(ret.getName());

    di.setDestination(dest);
    di.setOperationType(ADD_DEST);
    di.setConnectionId(cid);

    marshalCommand_(di, pendingBuffer_);

    return ret;
}

Destination
CoreLibImpl::createTopic(const std::string& name) {
    return Destination(parent_, name, false /* isTemp */, true /* isTopic */);
}

Destination
CoreLibImpl::createQueue(const std::string& name) {
    return Destination(parent_, name, false /* isTemp */, false /* isTopic */);
}
