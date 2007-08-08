/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.state;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
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

    Response processAddConnection(ConnectionInfo info) throws Exception;

    Response processAddSession(SessionInfo info) throws Exception;

    Response processAddProducer(ProducerInfo info) throws Exception;

    Response processAddConsumer(ConsumerInfo info) throws Exception;

    Response processRemoveConnection(ConnectionId id) throws Exception;

    Response processRemoveSession(SessionId id) throws Exception;

    Response processRemoveProducer(ProducerId id) throws Exception;

    Response processRemoveConsumer(ConsumerId id) throws Exception;

    Response processAddDestination(DestinationInfo info) throws Exception;

    Response processRemoveDestination(DestinationInfo info) throws Exception;

    Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Exception;

    Response processMessage(Message send) throws Exception;

    Response processMessageAck(MessageAck ack) throws Exception;

    Response processMessagePull(MessagePull pull) throws Exception;

    Response processBeginTransaction(TransactionInfo info) throws Exception;

    Response processPrepareTransaction(TransactionInfo info) throws Exception;

    Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception;

    Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception;

    Response processRollbackTransaction(TransactionInfo info) throws Exception;

    Response processWireFormat(WireFormatInfo info) throws Exception;

    Response processKeepAlive(KeepAliveInfo info) throws Exception;

    Response processShutdown(ShutdownInfo info) throws Exception;

    Response processFlush(FlushCommand command) throws Exception;

    Response processBrokerInfo(BrokerInfo info) throws Exception;

    Response processRecoverTransactions(TransactionInfo info) throws Exception;

    Response processForgetTransaction(TransactionInfo info) throws Exception;

    Response processEndTransaction(TransactionInfo info) throws Exception;

    Response processMessageDispatchNotification(MessageDispatchNotification notification) throws Exception;

    Response processProducerAck(ProducerAck ack) throws Exception;

    Response processMessageDispatch(MessageDispatch dispatch) throws Exception;

    Response processControlCommand(ControlCommand command) throws Exception;

    Response processConnectionError(ConnectionError error) throws Exception;

    Response processConnectionControl(ConnectionControl control) throws Exception;

    Response processConsumerControl(ConsumerControl control) throws Exception;

}
