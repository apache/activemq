/**
 *
 * Copyright 2004 The Apache Software Foundation
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
 */
package org.apache.activemq.state;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;

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

