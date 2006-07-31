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

#include "BrokerSession.h"
#include "BrokerSessionImpl_.h"

#include "amq_corelib/BlockingMessageConsumerRef.h"
#include "amq_corelib/NonBlockingMessageConsumerRef.h"
#include "amq_corelib/Logger.h"
#include "amq_corelib/RCSID.h"

using namespace ActiveMQ;
using namespace std;

RCSID(BrokerSession, "$Id$");

BrokerSession::BrokerSession(const string& uri,
                             const string& user,
                             const string& password)
    : impl_(new BrokerSessionImpl(uri, user, password))
  {}

void
BrokerSession::connect() {
    impl_->connect();
}

bool
BrokerSession::isConnected() const {
    return impl_->isConnected();
}

void
BrokerSession::disconnect() {
    impl_->disconnect();
}

void
BrokerSession::publish(const Message& msg) {
    impl_->publish(msg);
}

void
BrokerSession::publish(const Destination& d, const Message& msg) {
    impl_->publish(d, msg);
}

void
BrokerSession::subscribe(const Destination& dest, MessageConsumerRef& q) {
    impl_->subscribe(dest, q);
}

void
BrokerSession::unsubscribe(const Destination& dest) {
    impl_->unsubscribe(dest);
}

NonBlockingMessageConsumerRef
BrokerSession::newNonBlockingMessageConsumer() {
    return impl_->newNonBlockingMessageConsumer();
}

BlockingMessageConsumerRef
BrokerSession::newBlockingMessageConsumer() {
    return impl_->newBlockingMessageConsumer();
}

ExceptionCallback
BrokerSession::setExceptionCallback(ExceptionCallback c) {
    return impl_->setExceptionCallback(c);
}

void
BrokerSession::setLogger(auto_ptr<Logger> lgr) {
    impl_->setLogger(lgr);
}

void
BrokerSession::setLogger(Logger *lgr) {
    setLogger(auto_ptr<Logger>(lgr));
}

Destination
BrokerSession::createTopic(const std::string& name) {
    return impl_->createTopic(name);
}

Destination
BrokerSession::createQueue(const std::string& name) {
    return impl_->createQueue(name);
}

Destination
BrokerSession::createTemporaryTopic() {
    return impl_->createTemporaryTopic();
}

Destination
BrokerSession::createTemporaryQueue() {
    return impl_->createTemporaryQueue();
}
