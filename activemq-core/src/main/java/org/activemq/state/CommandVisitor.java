/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.state;

import org.activemq.command.BrokerInfo;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.DestinationInfo;
import org.activemq.command.FlushCommand;
import org.activemq.command.KeepAliveInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.ProducerId;
import org.activemq.command.ProducerInfo;
import org.activemq.command.RemoveSubscriptionInfo;
import org.activemq.command.Response;
import org.activemq.command.SessionId;
import org.activemq.command.SessionInfo;
import org.activemq.command.ShutdownInfo;
import org.activemq.command.TransactionInfo;
import org.activemq.command.WireFormatInfo;

public interface CommandVisitor {

    Response processAddConnection(ConnectionInfo info) throws Throwable;
    Response processAddSession(SessionInfo info) throws Throwable;
    Response processAddProducer(ProducerInfo info) throws Throwable;
    Response processAddConsumer(ConsumerInfo info) throws Throwable;
    
    Response processRemoveConnection(ConnectionId id) throws Throwable;
    Response processRemoveSession(SessionId id) throws Throwable;
    Response processRemoveProducer(ProducerId id) throws Throwable;
    Response processRemoveConsumer(ConsumerId id) throws Throwable;
    
    Response processAddDestination(DestinationInfo info) throws Throwable;
    Response processRemoveDestination(DestinationInfo info) throws Throwable;
    Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Throwable;
    
    Response processMessage(Message send) throws Throwable;
    Response processMessageAck(MessageAck ack) throws Throwable;

    Response processBeginTransaction(TransactionInfo info) throws Throwable;
    Response processPrepareTransaction(TransactionInfo info) throws Throwable;
    Response processCommitTransactionOnePhase(TransactionInfo info) throws Throwable;
    Response processCommitTransactionTwoPhase(TransactionInfo info) throws Throwable;
    Response processRollbackTransaction(TransactionInfo info) throws Throwable;

    Response processWireFormat(WireFormatInfo info) throws Throwable;
    Response processKeepAlive(KeepAliveInfo info) throws Throwable;
    Response processShutdown(ShutdownInfo info) throws Throwable;
    Response processFlush(FlushCommand command) throws Throwable;

    Response processBrokerInfo(BrokerInfo info) throws Throwable;
    Response processRecoverTransactions(TransactionInfo info) throws Throwable;
    Response processForgetTransaction(TransactionInfo info) throws Throwable;
    Response processEndTransaction(TransactionInfo info) throws Throwable;
    
}

